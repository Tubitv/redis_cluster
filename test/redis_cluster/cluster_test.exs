defmodule RedisCluster.ClusterTest do
  use ExUnit.Case, async: true

  setup_all do
    config =
      RedisCluster.Configuration.from_app_env(
        [
          otp_app: :none,
          host: "localhost",
          port: 6379,
          pool_size: 3
        ],
        RedisCluster.ClusterTest
      )

    case RedisCluster.Cluster.start_link(config) do
      {:ok, pid} ->
        pid

      {:error, {:already_started, pid}} ->
        pid
    end

    # Wait for cluster discovery
    Process.sleep(2000)

    {:ok, config: config}
  end

  test "config", context do
    config = context[:config]

    assert config.host == "localhost"
    assert config.port == 6379
    assert config.pool_size == 3
    assert config.name == RedisCluster.ClusterTest
    assert config.registry == RedisCluster.ClusterTest.Registry__
    assert config.cluster == RedisCluster.ClusterTest.Cluster__
    assert config.pool == RedisCluster.ClusterTest.Pool__
    assert config.shard_discovery == RedisCluster.ClusterTest.ShardDiscovery__
  end

  test "should handle basic operations", context do
    config = context[:config]

    key = "basic_ops_key"

    assert :ok = RedisCluster.Cluster.set(config, key, "test_value")
    assert "test_value" = RedisCluster.Cluster.get(config, key)
    assert 1 = RedisCluster.Cluster.delete(config, key)
    assert nil == RedisCluster.Cluster.get(config, key)
  end

  test "should get nil for non-existent key", context do
    config = context[:config]

    assert nil == RedisCluster.Cluster.get(config, "bogus")
  end

  test "should have expected topology", context do
    config = context[:config]

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

  test "should handle hash tags correctly", context do
    config = context[:config]

    # These keys should hash to the same slot due to the hash tag.
    pairs = %{
      "{user:123}:age" => 30,
      "{user:123}:email" => "john@example.com",
      "{user:123}:name" => "John"
    }

    keys = pairs |> Map.keys() |> Enum.sort()

    # Set values for all keys
    for {key, value} <- pairs do
      assert :ok = RedisCluster.Cluster.set(config, key, value, compute_hash_tag: true)
    end

    # Try a multi-key operation which should work since all keys hash to the same slot
    cmd = ["MGET" | keys]

    assert ["30", "john@example.com", "John"] ==
             RedisCluster.Cluster.command(config, cmd,
               key: List.first(keys),
               compute_hash_tag: true
             )

    # Clean up
    for key <- keys do
      RedisCluster.Cluster.delete(config, key, compute_hash_tag: true)
    end
  end

  test "should avoid cross-slot error with hash tags", context do
    config = context[:config]

    # Keys that will hash to different slots
    different_slot_keys = ["key1", "key2"]

    for {key, value} <- Enum.zip(different_slot_keys, ["value1", "value2"]) do
      assert :ok = RedisCluster.Cluster.set(config, key, value)
    end

    # MGET across slots should return an error
    cmd = ["MGET" | different_slot_keys]

    result =
      RedisCluster.Cluster.command(config, cmd,
        key: List.first(different_slot_keys),
        compute_hash_tag: true
      )

    assert match?({:error, _}, result)

    # Clean up
    for key <- different_slot_keys do
      RedisCluster.Cluster.delete(config, key)
    end
  end

  test "should handle large values", context do
    config = context[:config]
    key = "large_value_key"

    # Create a 1MB string
    large_value = String.duplicate("a", 1024 * 1024)

    assert :ok = RedisCluster.Cluster.set(config, key, large_value)
    assert ^large_value = RedisCluster.Cluster.get(config, key)

    # Clean up
    RedisCluster.Cluster.delete(config, key)
  end

  test "should handle special characters in keys", context do
    config = context[:config]

    special_keys = [
      "key:with:colons",
      "key-with-dashes",
      "key_with_underscores",
      "key with spaces",
      "key.with.dots",
      "key@with@at@signs",
      "key#with#hashes"
    ]

    for {key, index} <- Enum.with_index(special_keys) do
      value = "special_character_value_#{index}"
      assert :ok = RedisCluster.Cluster.set(config, key, value)
      assert value == RedisCluster.Cluster.get(config, key)
      assert 1 == RedisCluster.Cluster.delete(config, key)
    end
  end

  test "should handle seconds expiration option", context do
    config = context[:config]

    # Test setting with expiration in seconds
    key1 = "expiry_test_seconds"
    assert :ok = RedisCluster.Cluster.set(config, key1, "value1", expire_seconds: 1)
    assert "value1" = RedisCluster.Cluster.get(config, key1)

    # Wait for expiration
    Process.sleep(1500)
    assert nil == RedisCluster.Cluster.get(config, key1)
  end

  test "should handle milliseconds expiration option", context do
    config = context[:config]

    # Test setting with expiration in milliseconds
    key2 = "expiry_test_ms"
    assert :ok = RedisCluster.Cluster.set(config, key2, "value2", expire_milliseconds: 500)
    assert "value2" = RedisCluster.Cluster.get(config, key2)

    # Wait for expiration
    Process.sleep(1000)
    assert nil == RedisCluster.Cluster.get(config, key2)
  end

  test "should handle role-based operations", context do
    config = context[:config]
    key = "role_test_key"

    # Write to master
    assert :ok = RedisCluster.Cluster.set(config, key, "master_value", role: :master)

    # Read from master
    assert "master_value" = RedisCluster.Cluster.get(config, key, role: :master)

    # Give time for replication
    :timer.sleep(100)

    # Read from replica (should get the same value)
    assert "master_value" = RedisCluster.Cluster.get(config, key, role: :replica)

    # Clean up
    RedisCluster.Cluster.delete(config, key)
  end
end
