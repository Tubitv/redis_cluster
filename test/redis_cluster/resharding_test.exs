defmodule RedisCluster.ReshardingTest do
  use ExUnit.Case, async: false

  # TODO: Test resharding after a node is lost.

  alias RedisCluster.Cluster
  alias RedisCluster.ShardDiscovery

  setup_all do
    port = 8850

    config =
      RedisCluster.Configuration.from_app_env(
        [
          otp_app: :none,
          host: "localhost",
          port: port,
          pool_size: 3
        ],
        RedisCluster.DiscoveryTest
      )

    exec = Path.expand("../../scripts/redis_cluster.exs", __DIR__)

    System.cmd(exec, ~w[stop --port #{port} --purge-files])
    System.cmd(exec, ~w[start --port #{port} --replicas-per-master 3])

    # Wait for cluster creation
    Process.sleep(5000)

    case Cluster.start_link(config) do
      {:ok, pid} ->
        pid

      {:error, {:already_started, pid}} ->
        pid
    end

    # Wait for cluster discovery
    Process.sleep(2000)

    {:ok, config: config, port: port}
  end

  test "should not have duplicated nodes after manually rediscovering", context do
    config = context[:config]
    port = context[:port]

    check_slots(config)

    System.cmd("redis-cli", ["-p", to_string(port + 1), "shutdown"])

    # Wait for shutdown
    Process.sleep(2000)

    ShardDiscovery.rediscover_shards(config)

    # Wait for cluster discovery
    Process.sleep(2000)

    slots = config |> RedisCluster.HashSlots.all_slots() |> Enum.sort()

    assert [
             {RedisCluster.HashSlots, 0, 5460, :master, "127.0.0.1", _},
             {RedisCluster.HashSlots, 0, 5460, :replica, "127.0.0.1", _},
             {RedisCluster.HashSlots, 0, 5460, :replica, "127.0.0.1", _},
             {RedisCluster.HashSlots, 0, 5460, :replica, "127.0.0.1", _},
             {RedisCluster.HashSlots, 5461, 10922, :master, "127.0.0.1", _},
             {RedisCluster.HashSlots, 5461, 10922, :replica, "127.0.0.1", _},
             {RedisCluster.HashSlots, 5461, 10922, :replica, "127.0.0.1", _},
             {RedisCluster.HashSlots, 5461, 10922, :replica, "127.0.0.1", _},
             {RedisCluster.HashSlots, 10923, 16383, :master, "127.0.0.1", _},
             {RedisCluster.HashSlots, 10923, 16383, :replica, "127.0.0.1", _},
             {RedisCluster.HashSlots, 10923, 16383, :replica, "127.0.0.1", _}
           ] = slots
  end

  defp check_slots(config) do
    slots = config |> RedisCluster.HashSlots.all_slots() |> Enum.sort()

    assert [
             {RedisCluster.HashSlots, 0, 5460, :master, "127.0.0.1", _},
             {RedisCluster.HashSlots, 0, 5460, :replica, "127.0.0.1", _},
             {RedisCluster.HashSlots, 0, 5460, :replica, "127.0.0.1", _},
             {RedisCluster.HashSlots, 0, 5460, :replica, "127.0.0.1", _},
             {RedisCluster.HashSlots, 5461, 10922, :master, "127.0.0.1", _},
             {RedisCluster.HashSlots, 5461, 10922, :replica, "127.0.0.1", _},
             {RedisCluster.HashSlots, 5461, 10922, :replica, "127.0.0.1", _},
             {RedisCluster.HashSlots, 5461, 10922, :replica, "127.0.0.1", _},
             {RedisCluster.HashSlots, 10923, 16383, :master, "127.0.0.1", _},
             {RedisCluster.HashSlots, 10923, 16383, :replica, "127.0.0.1", _},
             {RedisCluster.HashSlots, 10923, 16383, :replica, "127.0.0.1", _},
             {RedisCluster.HashSlots, 10923, 16383, :replica, "127.0.0.1", _}
           ] = slots
  end
end
