defmodule RedisCluster.ShardDiscovery do
  use GenServer

  require Logger

  alias RedisCluster.Cluster.NodeInfo
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
      info = cluster_info(config)

      Logger.debug("Found cluster info\n#{NodeInfo.to_table(info)}")

      stop_pool(config)
      create_pool(config, info)

      slots = Enum.flat_map(info, & &1.slots)

      HashSlots.add_slots(config, slots)
    end)
  end

  @spec cluster_info(Configuration.t()) :: [NodeInfo.t()] | no_return()
  defp cluster_info(config) do
    {:ok, conn} = Redix.start_link(host: config.host, port: config.port)

    info = fetch_cluster_info(conn)

    # Do I need to stop the connection here?
    Process.exit(conn, :normal)

    info
  end

  defp fetch_cluster_info(conn) do
    cluster_shards(conn) || cluster_slots(conn)
  end

  defp cluster_shards(conn) do
    case Redix.command(conn, ~w[CLUSTER SHARDS]) do
      {:ok, data} ->
        RedisCluster.Cluster.ShardParser.parse(data)

      {:error, _} ->
        nil
    end
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

  defp cluster_slots(conn) do
    case Redix.command(conn, ~w[CLUSTER SLOTS]) do
      {:ok, data} ->
        RedisCluster.Cluster.SlotParser.parse(data)

      {:error, _} ->
        nil
    end
  end
end
