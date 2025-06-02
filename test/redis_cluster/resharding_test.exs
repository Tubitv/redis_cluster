defmodule RedisCluster.ReshardingTest do
  use ExUnit.Case, async: true

  alias RedisCluster.Cluster
  alias RedisCluster.ClusterInfo
  alias RedisCluster.HashSlots
  alias RedisCluster.ShardDiscovery

  @moduletag :slow

  # These tests aren't quite working yet.
  @moduletag :skip

  describe "when terminating a replica" do
    setup do
      {config, port} = setup_cluster(RedisCluster.ReshardTest, 8850)

      {:ok, config: config, port: port}
    end

    test "should not include replica after it is removed", context do
      config = context[:config]
      key = "test_replica"
      slot = RedisCluster.Key.hash_slot(key)

      [target_port] =
        for {_key, start, stop, :replica, _host, port} <- HashSlots.all_slots(config),
            slot >= start and slot < stop do
          port
        end

      check_slots(config)

      stop_port(target_port)

      # Wait for shutdown
      wait_for_fail(config, 10)

      # Fail to query terminated replica.
      assert {:error, %Redix.ConnectionError{reason: :closed}} =
               RedisCluster.Cluster.get(config, key, role: :replica)

      ShardDiscovery.rediscover_shards(config)

      online_ports =
        for {_key, _start, _stop, _role, _host, port} <- HashSlots.all_slots(config) do
          port
        end

      assert target_port not in online_ports
      assert [masters: 6, replicas: 5] = count_roles(config)
    end
  end

  describe "when terminating a master" do
    setup do
      {config, port} = setup_cluster(RedisCluster.ReshardTest, 8880)

      {:ok, config: config, port: port}
    end

    @tag timeout: 120_000
    test "should promote new master when terminated", context do
      config = context[:config]
      key = "test_master"
      slot = RedisCluster.Key.hash_slot(key)

      [{master_start, master_stop, master_port}] =
        for {_key, start, stop, :master, _host, port} <- HashSlots.all_slots(config),
            slot >= start and slot < stop do
          {start, stop, port}
        end

      [replica_port | _] =
        for {_key, ^master_start, ^master_stop, :replica, _host, port} <-
              HashSlots.all_slots(config) do
          port
        end

      # check_slots(config)

      IO.puts("Stopping master port #{master_port}")

      stop_port(master_port)

      # Wait for shutdown
      wait_for_fail(config, 10)

      # Fail to query terminated replica.
      assert {:error, %Redix.ConnectionError{reason: :closed}} =
               RedisCluster.Cluster.get(config, key, role: :master)

      ShardDiscovery.rediscover_shards(config)

      online_ports =
        for {_key, _start, _stop, _role, _host, port} <- HashSlots.all_slots(config) do
          port
        end

      assert master_port not in online_ports

      assert [masters: 3, replicas: _] = count_roles(config)

      IO.puts("Force failover to replica port #{replica_port}")

      # Force the replica to become a master.
      force_failover(replica_port)

      wait_for_master_count(config, 4, 60)

      ShardDiscovery.rediscover_shards(config)

      Process.sleep(3000)

      assert [masters: 4, replicas: 5] = count_roles(config)
    end
  end

  ## Helpers

  defp setup_cluster(module, port) do
    config =
      RedisCluster.Configuration.from_app_env(
        [
          otp_app: :none,
          host: "localhost",
          port: port,
          pool_size: 1
        ],
        module
      )

    exec = Path.expand("../../scripts/redis_cluster.exs", __DIR__)

    System.cmd(exec, ~w[stop --port #{port} --purge-files])
    System.cmd(exec, ~w[start --port #{port} --replicas-per-master 2])

    # Wait for cluster creation
    wait_for_ready(config, 30)

    case Cluster.start_link(config) do
      {:ok, pid} ->
        pid

      {:error, {:already_started, pid}} ->
        pid
    end

    wait_for_discovery(config, 20)

    {config, port}
  end

  defp check_slots(config) do
    slots = config |> RedisCluster.HashSlots.all_slots() |> Enum.sort()

    assert [
             {RedisCluster.HashSlots, 0, 2730, :master, "127.0.0.1", _},
             {RedisCluster.HashSlots, 0, 2730, :replica, "127.0.0.1", _},
             {RedisCluster.HashSlots, 2731, 5460, :master, "127.0.0.1", _},
             {RedisCluster.HashSlots, 2731, 5460, :replica, "127.0.0.1", _},
             {RedisCluster.HashSlots, 5461, 8191, :master, "127.0.0.1", _},
             {RedisCluster.HashSlots, 5461, 8191, :replica, "127.0.0.1", _},
             {RedisCluster.HashSlots, 8192, 10922, :master, "127.0.0.1", _},
             {RedisCluster.HashSlots, 8192, 10922, :replica, "127.0.0.1", _},
             {RedisCluster.HashSlots, 10923, 13652, :master, "127.0.0.1", _},
             {RedisCluster.HashSlots, 10923, 13652, :replica, "127.0.0.1", _},
             {RedisCluster.HashSlots, 13653, 16383, :master, "127.0.0.1", _},
             {RedisCluster.HashSlots, 13653, 16383, :replica, "127.0.0.1", _}
           ] = slots
  end

  defp wait_for_discovery(_config, 0) do
    :failed
  end

  defp wait_for_discovery(config, retries) do
    if HashSlots.all_slots(config) == [] do
      Process.sleep(100)
      wait_for_discovery(config, retries - 1)
    else
      :ok
    end
  end

  defp wait_for_ready(config, retries) do
    ClusterInfo.query_while(config, fn info, attempt ->
      if attempt > retries do
        raise "Cluster not ready after #{attempt} attempts"
      end

      ready? =
        Enum.all?(info, fn node ->
          node.health in [:online, :unknown]
        end)

      has_replicas? = Enum.any?(info, fn node -> node.role == :replica end)

      retry? = not ready? or not has_replicas?

      if retry? do
        Process.sleep(1000)
      end

      retry?
    end)
  end

  defp wait_for_fail(config, retries) do
    ClusterInfo.query_while(config, fn info, attempt ->
      if attempt > retries do
        raise "Cluster not failed after #{attempt} attempts"
      end

      fail? =
        Enum.any?(info, fn node ->
          node.health == :failed
        end)

      if not fail? do
        Process.sleep(1000)
      end

      not fail?
    end)
  end

  defp wait_for_master_count(config, expected_count, retries) do
    ClusterInfo.query_while(config, fn info, attempt ->
      if attempt > retries do
        raise "Cluster not failed after #{attempt} attempts"
      end

      IO.puts("Nodes: #{RedisCluster.Cluster.NodeInfo.to_table(info)}")

      # info
      # |> Enum.filter(fn node ->
      # node.role == :master
      # end)
      # |> IO.inspect(label: :masters)

      count = Enum.count(info, &(&1.role == :master && &1.health == :online))
      # failed? = Enum.any?(info, fn node -> node.health == :failed end)

      # and not failed?
      pass? = count == expected_count

      if not pass? do
        Process.sleep(1000)
      end

      not pass?
    end)
  end

  defp stop_port(port) do
    System.cmd("redis-cli", ["-p", to_string(port), "shutdown"])
  end

  defp count_roles(config) do
    nodes_by_role =
      config
      |> HashSlots.all_slots()
      |> Enum.group_by(fn {_key, _start, _stop, role, _host, _port} ->
        role
      end)

    masters = Map.get(nodes_by_role, :master, [])
    replicas = Map.get(nodes_by_role, :replica, [])

    [masters: length(masters), replicas: length(replicas)]
  end

  defp force_failover(port) do
    System.cmd("redis-cli", ["-p", to_string(port), "CLUSTER", "FAILOVER", "FORCE"])
    |> IO.inspect(label: :failover)
  end
end
