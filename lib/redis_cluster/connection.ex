defmodule RedisCluster.Connection do
  @moduledoc """
  A shim module to start a Redix connection.
  This is necessary to send the [READONLY command](https://redis.io/docs/latest/commands/readonly/) to replicas.
  When in read-write mode, replicas will redirect to the master.
  This must be done per connection, not node.
  """

  alias RedisCluster.Configuration
  alias RedisCluster.Telemetry

  @doc false
  @spec start_link(
          {role :: :master | :replica, config :: Configuration.t(), conn_opts :: Keyword.t()}
        ) :: {:ok, pid()}
  def start_link({role, config, conn_opts}) do
    {:ok, pid} = config.redis_module.start_link(conn_opts)

    _ =
      if role == :replica do
        config.redis_module.command!(pid, ["READONLY"])
        attach_readonly_resend_handler(pid, config)
      end

    {:ok, pid}
  end

  defp attach_readonly_resend_handler(pid, config) do
    handler_id = {__MODULE__, pid}

    _ =
      :telemetry.attach(
        handler_id,
        [:redix, :connection],
        &__MODULE__.handle_redix_connection/4,
        %{pid: pid, config: config}
      )

    spawn(fn ->
      ref = Process.monitor(pid)

      receive do
        {:DOWN, ^ref, :process, ^pid, _reason} -> :telemetry.detach(handler_id)
      end
    end)
  end

  @doc false
  def handle_redix_connection(_event, _measurements, %{connection: pid}, %{
        pid: pid,
        config: config
      }) do
    # Must run in a separate process: this handler fires synchronously inside
    # the Redix connection's own :gen_statem callback while it is reconnecting.
    # Calling command!/2 on `pid` from `pid` itself would deadlock, since
    # Redix's client-side receive can only be satisfied once the connection
    # process returns to its main loop.
    spawn(fn ->
      config.redis_module.command!(pid, ["READONLY"])
      Telemetry.readonly_resent(%{pid: pid})
    end)
  end

  def handle_redix_connection(_event, _measurements, _metadata, _handler_config), do: :ok

  @doc false
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]}
    }
  end
end
