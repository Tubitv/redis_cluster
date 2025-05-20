defmodule RedisCluster.Pool do
  @moduledoc """
  Redis Cluster Node/Slot Supervisor.
  """

  use DynamicSupervisor

  alias RedisCluster.Cluster.NodeInfo
  alias RedisCluster.Configuration

  @doc false
  def start_link(config) do
    DynamicSupervisor.start_link(__MODULE__, config, name: config.pool)
  end

  @impl DynamicSupervisor
  def init(_config) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @spec stop_pool(Configuration.t()) :: :ok
  def stop_pool(config) do
    name = config.pool

    for {_id, pid, _type, _modules} when is_pid(pid) <-
          DynamicSupervisor.which_children(name) do
      _ = DynamicSupervisor.terminate_child(name, pid)
    end

    :ok
  end

  @spec start_pool(Configuration.t(), NodeInfo.t()) :: :ok
  def start_pool(config, %NodeInfo{host: host, port: port, role: role}) do
    supervisor_name = config.pool

    for index <- 1..config.pool_size do
      name = registry_name(config.registry, host, port, index - 1)

      conn_opts = [host: host, port: port, name: name]
      args = {role, conn_opts}

      spec = Supervisor.child_spec({RedisCluster.Connection, args}, id: {Redix, name})

      _ = DynamicSupervisor.start_child(supervisor_name, spec)
    end

    :ok
  end

  @spec get_conn(
          Configuration.t(),
          role :: :master | :replica,
          port :: non_neg_integer()
        ) :: pid()
  def get_conn(config, host, port) when is_binary(host) and is_integer(port) do
    require Logger
    # Ensure selecting the same connection based on the caller PID
    index = :erlang.phash2(self(), config.pool_size)
    key = {host, port, index}

    [{pid, _value} | _] = Registry.lookup(config.registry, key)

    pid
  end

  defp registry_name(registry, host, port, index) do
    {:via, Registry, {registry, {host, port, index}}}
  end
end
