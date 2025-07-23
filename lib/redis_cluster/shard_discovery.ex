defmodule RedisCluster.ShardDiscovery do
  @moduledoc """
  A GenServer that discovers Redis shards in a cluster and manages the connection pools.
  This module doesn't actively monitor the cluster for changes. Instead it waits for a
  call to `rediscover_shards/1` to trigger a discovery process. This generally happens
  when a MOVED error is encountered, indicating the cluster topology has changed.
  """

  use GenServer

  require Logger

  alias RedisCluster.Cluster.NodeInfo
  alias RedisCluster.ClusterInfo
  alias RedisCluster.Configuration
  alias RedisCluster.HashSlots
  alias RedisCluster.Telemetry

  @doc false
  def start_link(config) do
    GenServer.start_link(__MODULE__, config, name: config.shard_discovery)
  end

  @impl GenServer
  def init(config) do
    _ = HashSlots.create_table(config)
    {:ok, config, {:continue, :discover_shards}}
  end

  @impl GenServer
  def handle_continue(:discover_shards, config) do
    discover_shards(config)
    {:noreply, config}
  end

  @impl GenServer
  def handle_call(:discover_shards, _from, config) do
    discover_shards(config)
    {:reply, :ok, config}
  end

  @doc """
  Triggers a rediscovery of the shards in the cluster. You shouldn't need to call this
  function directly in most cases, as the cluster will automatically handle shard 
  discovery when needed.
  """
  @spec rediscover_shards(Configuration.t()) :: :ok
  def rediscover_shards(config) do
    Telemetry.cluster_rediscovery(%{config_name: config.name})
    GenServer.call(config.shard_discovery, :discover_shards)
  end

  ## Helpers

  defp discover_shards(config) do
    Logger.debug("Discovering shards for #{config.name}")

    metadata = %{
      config_name: config.name
    }

    Telemetry.execute_discovery(metadata, fn ->
      HashSlots.with_lock(config, fn ->
        info = ClusterInfo.query(config)

        Logger.debug("Found cluster info\n#{NodeInfo.to_table(info)}")

        online_nodes =
          Enum.filter(info, fn node_info ->
            node_info.health in [:online, :unknown]
          end)

        stop_pool(config)
        create_pool(config, online_nodes)

        slots = Enum.flat_map(online_nodes, & &1.slots)

        HashSlots.delete(config)
        HashSlots.add_slots(config, slots)

        # Return useful information for telemetry
        %{
          total_nodes: length(info),
          online_nodes: length(online_nodes),
          total_slots: length(slots)
        }
      end)
    end)
  end

  defp stop_pool(config) do
    RedisCluster.Pool.stop_pool(config)
  end

  defp create_pool(config, info) do
    for node_info <- info do
      RedisCluster.Pool.start_pool(config, node_info)
    end

    :ok
  end
end
