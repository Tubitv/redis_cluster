defmodule RedisCluster.ConnectionTest do
  use ExUnit.Case, async: true

  alias RedisCluster.Configuration
  alias RedisCluster.Connection

  defmodule MockRedisModule do
    use GenServer

    def start_link(opts) do
      test_pid = Keyword.fetch!(opts, :test_pid)
      GenServer.start_link(__MODULE__, test_pid)
    end

    def command!(pid, cmd) do
      GenServer.call(pid, {:command, cmd})
    end

    @impl true
    def init(test_pid) do
      {:ok, test_pid}
    end

    @impl true
    def handle_call({:command, cmd}, _from, test_pid) do
      send(test_pid, {:command, self(), cmd})
      {:reply, :ok, test_pid}
    end
  end

  setup do
    config = %Configuration{
      host: "localhost",
      port: 6379,
      name: :test_cluster,
      registry: :test_registry,
      cluster: :test_cluster_name,
      pool: :test_pool,
      shard_discovery: :test_discovery,
      pool_size: 1,
      redis_module: MockRedisModule,
      ssl: false,
      ssl_opts: []
    }

    {:ok, config: config}
  end

  test "starting a :replica connection sends READONLY once immediately", %{config: config} do
    {:ok, pid} = Connection.start_link({:replica, config, test_pid: self()})

    assert_received {:command, ^pid, ["READONLY"]}
    refute_received {:command, ^pid, ["READONLY"]}
  end

  test "re-emitting the redix connection event re-sends READONLY", %{config: config} do
    {:ok, pid} = Connection.start_link({:replica, config, test_pid: self()})

    assert_received {:command, ^pid, ["READONLY"]}

    :telemetry.attach(
      :readonly_resent_test_handler,
      [:redis_cluster, :connection, :readonly_resent],
      &__MODULE__.handle_readonly_resent/4,
      %{test_pid: self()}
    )

    on_exit(fn -> :telemetry.detach(:readonly_resent_test_handler) end)

    :telemetry.execute([:redix, :connection], %{}, %{connection: pid, reconnection: true})

    assert_receive {:command, ^pid, ["READONLY"]}
    assert_receive {:readonly_resent, %{pid: ^pid}}
  end

  test "an event for a different pid does not trigger a resend", %{config: config} do
    {:ok, pid} = Connection.start_link({:replica, config, test_pid: self()})

    assert_received {:command, ^pid, ["READONLY"]}

    other_pid = spawn(fn -> :ok end)
    :telemetry.execute([:redix, :connection], %{}, %{connection: other_pid, reconnection: true})

    refute_received {:command, ^pid, ["READONLY"]}
  end

  test "killing the connection process detaches the handler", %{config: config} do
    Process.flag(:trap_exit, true)
    {:ok, pid} = Connection.start_link({:replica, config, test_pid: self()})

    handler_id = {Connection, pid}
    assert Enum.any?(:telemetry.list_handlers([:redix, :connection]), &(&1.id == handler_id))

    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}

    assert wait_until(fn ->
             not Enum.any?(
               :telemetry.list_handlers([:redix, :connection]),
               &(&1.id == handler_id)
             )
           end)
  end

  def handle_readonly_resent(_event, _measurements, metadata, %{test_pid: test_pid}) do
    send(test_pid, {:readonly_resent, metadata})
  end

  defp wait_until(fun, attempts \\ 20) do
    cond do
      fun.() ->
        true

      attempts <= 0 ->
        false

      true ->
        Process.sleep(5)
        wait_until(fun, attempts - 1)
    end
  end

  test ":master-role connections are unaffected", %{config: config} do
    {:ok, pid} = Connection.start_link({:master, config, test_pid: self()})

    refute_received {:command, ^pid, ["READONLY"]}

    handler_id = {Connection, pid}
    refute Enum.any?(:telemetry.list_handlers([:redix, :connection]), &(&1.id == handler_id))
  end
end
