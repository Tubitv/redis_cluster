defmodule RedisCluster.Pool do
  @moduledoc """
  Redis Cluster connection supervisor. A "pool" in this context is a set of connections
  that point to the same Redis node (host and port). A Redis cluster will have a pool of
  connections for each node in the cluster.
  """

  use DynamicSupervisor

  alias RedisCluster.Cluster.NodeInfo
  alias RedisCluster.Configuration
  alias RedisCluster.Telemetry

  @doc false
  def start_link(config) do
    DynamicSupervisor.start_link(__MODULE__, config, name: config.pool)
  end

  @impl DynamicSupervisor
  def init(_config) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @doc """
  Stops all connections in the pool. This is typically called when the application
  rediscovers the cluster.
  """
  @spec stop_pool(Configuration.t()) :: :ok
  def stop_pool(config) do
    name = config.pool

    for {_id, pid, _type, _modules} when is_pid(pid) <-
          DynamicSupervisor.which_children(name) do
      _ = DynamicSupervisor.terminate_child(name, pid)
    end

    :ok
  end

  @doc """
  Starts a pool of connections for the given node information.
  The configuration specifies how many connections to start for the given node.
  """
  @spec start_pool(Configuration.t(), NodeInfo.t()) :: :ok
  def start_pool(config, %NodeInfo{host: host, port: port, role: role}) do
    supervisor_name = config.pool

    for index <- 1..config.pool_size do
      name = registry_name(config.registry, host, port, index - 1)

      conn_opts = [host: host, port: port, name: name]
      args = {role, config, conn_opts}

      spec = Supervisor.child_spec({RedisCluster.Connection, args}, id: {Redix, name})

      _ = DynamicSupervisor.start_child(supervisor_name, spec)
    end

    :ok
  end

  @doc """
  Retrieves a connection from the pool based on the host and port.
  Will return the same connection for the same host and port per process.
  """
  @spec get_conn(
          Configuration.t(),
          host :: binary(),
          port :: non_neg_integer()
        ) :: pid()
  def get_conn(config, host, port) when is_binary(host) and is_integer(port) do
    require Logger
    # Ensure selecting the same connection based on the caller PID
    index = :erlang.phash2(self(), config.pool_size)
    key = {host, port, index}

    [{pid, _value} | _] = Registry.lookup(config.registry, key)

    # Emit telemetry event for connection acquisition
    Telemetry.connection_acquired(%{
      config_name: config.name,
      host: host,
      port: port,
      index: index,
      pid: pid
    })

    pid
  end

  defp registry_name(registry, host, port, index) do
    {:via, Registry, {registry, {host, port, index}}}
  end
end
