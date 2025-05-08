defmodule RedisCluster.ShardDiscovery do
  use GenServer

  require Logger

  alias RedisCluster.Cluster.NodeInfo
  alias RedisCluster.ClusterInfo
  alias RedisCluster.Configuration
  alias RedisCluster.HashSlots

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

  @spec rediscover_shards(Configuration.t()) :: :ok
  def rediscover_shards(config) do
    GenServer.call(config.shard_discovery, :discover_shards)
  end

  ## Helpers

  defp discover_shards(config) do
    Logger.debug("Discovering shards for #{config.name}")

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
