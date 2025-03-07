defmodule RedisCluster.Pool do
  @moduledoc """
  Redis Cluster Node/Slot Supervisor.
  """

  use DynamicSupervisor

  alias RedisCluster.Pool
  alias RedisCluster.Cluster.NodeInfo

  ## API

  @doc false
  def start_link(config) do
    DynamicSupervisor.start_link(__MODULE__, config, name: name(config))
  end

  def name(config) do
    Module.concat(config.name, "PoolSupervisor__")
  end

  @impl DynamicSupervisor
  def init(_config) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  def stop_pool(config) do
    name = name(config)

    for {_id, pid, _type, _modules} when is_pid(pid) <-
          DynamicSupervisor.which_children(name) do
      DynamicSupervisor.terminate_child(name, pid)
    end
  end

  require Logger

  def start_pool(config, node_info = %NodeInfo{}) do
    Logger.debug("Starting pool for #{config.name} #{inspect(node_info)}")

    # ???: Do we need other options?

    # Creates redundant connections to a node if it has multiple slot ranges. 
    # It would be nice to consolidate connections but that complicates name registration/supervision.

    supervisor_name = name(config)

    for index <- 1..config.pool_size, slot_id <- node_info.slots do
      name = registry_name(config.registry, slot_id, node_info.role, index - 1)
      Logger.debug("Creating connection #{inspect(name)}")

      conn_opts = [
        host: node_info.host,
        port: node_info.port,
        name: name
      ]

      spec = Supervisor.child_spec({Redix, conn_opts}, id: {Redix, name})

      DynamicSupervisor.start_child(supervisor_name, spec)
    end
  end

  def get_conn(config, role, slot_id) do
    # Ensure selecting the same connection based on the caller PID
    index = :erlang.phash2(self(), config.pool_size)
    key = {slot_id, role, index}

    Logger.debug("Getting conn for #{inspect(key)}")

    [{pid, _value} | _] = Registry.lookup(config.registry, key)

    pid
  end

  defp registry_name(registry, slot_id, role, index) do
    {:via, Registry, {registry, {slot_id, role, index}}}
  end
end
