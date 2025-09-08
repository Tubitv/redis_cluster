defmodule RedisCluster.ShardDiscovery do
  @moduledoc """
  A GenServer that discovers Redis shards in a cluster and manages the connection pools.
  This module doesn't actively monitor the cluster for changes. Instead it waits for a
  call to `rediscover_shards/1` to trigger a discovery process. This generally happens
  when a MOVED redirect is encountered, indicating the cluster topology has changed.
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
    state = %{config: config, discovery_state: :idle}
    {:ok, state, {:continue, :discover_shards}}
  end

  @impl GenServer
  def handle_continue(:discover_shards, state) do
    discover_shards(state.config)
    {:noreply, state}
  end

  @impl GenServer
  def handle_call(:discover_shards, from, state) do
    case state.discovery_state do
      :idle ->
        new_state = %{state | discovery_state: {:discovering, [from]}}
        perform_discovery_async(new_state)
        {:noreply, new_state}

      {:discovering, waiting_callers} ->
        # Add caller to waiting list, they'll get a reply when discovery completes
        new_state = %{state | discovery_state: {:discovering, [from | waiting_callers]}}
        {:noreply, new_state}
    end
  end

  @impl GenServer
  def handle_cast(:discover_shards_async, state) do
    case state.discovery_state do
      :idle ->
        new_state = %{state | discovery_state: {:discovering, []}}
        perform_discovery_async(new_state)
        {:noreply, new_state}

      _ ->
        # Discovery already in progress, ignore duplicate request
        {:noreply, state}
    end
  end

  @impl GenServer
  def handle_info(:discovery_complete, state) do
    case state.discovery_state do
      {:discovering, waiting_callers} ->
        Enum.each(waiting_callers, &GenServer.reply(&1, :ok))

        new_state = %{state | discovery_state: :idle}
        {:noreply, new_state}

      _ ->
        {:noreply, state}
    end
  end

  @doc """
  Triggers a rediscovery of the shards in the cluster. You shouldn't need to call this
  function directly in most cases, as the cluster will automatically handle shard
  discovery when needed.

  This function waits for the discovery to complete before returning.
  """
  @spec rediscover_shards(Configuration.t()) :: :ok
  def rediscover_shards(config) do
    Telemetry.cluster_rediscovery(%{config_name: config.name})
    GenServer.call(config.shard_discovery, :discover_shards)
  end

  @doc """
  Triggers a rediscovery of the shards in the cluster asynchronously. This function
  returns immediately without waiting for the discovery to complete.

  If a discovery is already in progress, this call is ignored (no duplicate work).
  """
  @spec rediscover_shards_async(Configuration.t()) :: :ok
  def rediscover_shards_async(config) do
    Telemetry.cluster_rediscovery(%{config_name: config.name})
    GenServer.cast(config.shard_discovery, :discover_shards_async)
  end

  ## Helpers

  defp perform_discovery_async(state) do
    parent_pid = self()

    Task.start(fn ->
      try do
        discover_shards(state.config)
      rescue
        error -> {:error, error}
      catch
        :exit, reason -> {:error, {:exit, reason}}
        :throw, value -> {:error, {:throw, value}}
      end

      send(parent_pid, :discovery_complete)
    end)
  end

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

        slots = Enum.flat_map(online_nodes, & &1.slots)

        if slots == [] do
          Logger.error("No slots found in cluster for #{config.name}")
          rediscover_shards(config)
        else
          stop_pool(config)
          create_pool(config, online_nodes)

          HashSlots.delete(config)
          HashSlots.add_slots(config, slots)
        end

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
