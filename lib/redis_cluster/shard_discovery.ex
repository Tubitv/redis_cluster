defmodule RedisCluster.ShardDiscovery do
  use GenServer

  alias RedisCluster.HashSlots

  def start_link(config) do
    GenServer.start_link(__MODULE__, config, name: name(config))
  end

  def name(config) do
    Module.concat(config.name, "ShardDiscovery__")
  end

  def init(config) do
    HashSlots.create_table(config.name)
    {:ok, config, {:continue, :discover_shards}}
  end

  @impl GenServer
  def handle_continue(:discover_shards, config) do
    discover_shards(config)
    {:noreply, config}
  end

  @impl GenServer
  def handle_call(:discover_shards, from, config) do
    discover_shards(config)
    {:reply, :ok, config}
  end

  def rediscover_shards(config) do
    GenServer.call(name(config), :discover_shards)
  end

  ## Helpers

  # TODO: Make the helpers private

  require Logger

  defp discover_shards(config) do
    Logger.debug("Discovering shards for #{config.name}")

    HashSlots.with_lock(config.name, fn ->
      info = cluster_info(config)

      Logger.debug("Found cluster info #{inspect(info)}")

      stop_pool(config)
      create_pool(config, info)

      slots = Enum.flat_map(info, & &1.slots)

      HashSlots.add_slots(config.name, slots)
    end)
  end

  def cluster_info(config) do
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
      Logger.debug("slots #{inspect(node_info.slots)}")
      RedisCluster.Pool.start_pool(config, node_info)
    end
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
