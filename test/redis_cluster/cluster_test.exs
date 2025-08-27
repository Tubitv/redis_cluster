defmodule RedisCluster.ClusterTest do
  use ExUnit.Case, async: true

  alias RedisCluster.Cluster

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

    case Cluster.start_link(config) do
      {:ok, pid} ->
        pid

      {:error, {:already_started, pid}} ->
        pid
    end

    # Wait for cluster discovery
    Process.sleep(2000)

    {:ok, config: config}
  end

  describe "bookkeeping operations" do
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
  end

  describe "single-key operations" do
    test "should handle basic operations", context do
      config = context[:config]

      key = "basic_ops_key"

      assert :ok = Cluster.set(config, key, "test_value")
      assert "test_value" = Cluster.get(config, key)
      assert 1 = Cluster.delete(config, key)
      assert nil == Cluster.get(config, key)
    end

    test "should get nil for non-existent key", context do
      config = context[:config]

      assert nil == Cluster.get(config, "bogus")
    end

    test "should handle large values", context do
      config = context[:config]
      key = "large_value_key"

      # Create a 1MB string
      large_value = String.duplicate("a", 1024 * 1024)

      assert :ok = Cluster.set(config, key, large_value)
      assert ^large_value = Cluster.get(config, key)

      # Clean up
      Cluster.delete(config, key)
    end

    test "should get ok when deleting with no reply", context do
      config = context[:config]

      key = "delete-noreply-key"
      value = "value"

      assert :ok = Cluster.set(config, key, value)
      assert value == Cluster.get(config, key)
      assert :ok = Cluster.delete_noreply(config, key)
      assert nil == Cluster.get(config, key)
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
        assert :ok = Cluster.set(config, key, value)
        assert value == Cluster.get(config, key)
        assert 1 == Cluster.delete(config, key)
      end
    end

    test "should handle binary keys", context do
      config = context[:config]

      key = <<0xDE, 0xAD, 0xC0, 0xDE>>
      value = "value"

      assert :ok = Cluster.set(config, key, value)
      assert value == Cluster.get(config, key)
      assert 1 == Cluster.delete(config, key)
    end

    test "should handle seconds expiration option", context do
      config = context[:config]

      # Test setting with expiration in seconds
      key = "expiry_test_seconds"
      assert :ok = Cluster.set(config, key, "value1", expire_seconds: 1)
      assert "value1" = Cluster.get(config, key)

      # Wait for expiration
      Process.sleep(1100)
      assert nil == Cluster.get(config, key)
    end

    test "should handle milliseconds expiration option", context do
      config = context[:config]

      # Test setting with expiration in milliseconds
      key = "expiry_test_ms"
      assert :ok = Cluster.set(config, key, "value2", expire_milliseconds: 100)
      assert "value2" = Cluster.get(config, key)

      # Wait for expiration
      Process.sleep(200)
      assert nil == Cluster.get(config, key)
    end

    test "should handle role-based operations", context do
      config = context[:config]
      key = "role_test_key"

      # Write to master
      assert :ok = Cluster.set(config, key, "master_value", role: :master)

      # Read from master
      assert "master_value" = Cluster.get(config, key, role: :master)

      # Give time for replication
      :timer.sleep(100)

      # Read from replica (should get the same value)
      assert "master_value" = Cluster.get(config, key, role: :replica)

      # Clean up
      Cluster.delete(config, key)
    end

    test "should overwrite when instructed", context do
      config = context[:config]
      key = "overwrite_test_key"

      # Fail to set initial value
      assert nil == Cluster.set(config, key, "initial_value", set: :only_overwrite)
      assert nil == Cluster.get(config, key)

      # Actually set initial value
      assert :ok = Cluster.set(config, key, "initial_value")
      assert "initial_value" = Cluster.get(config, key)

      # Overwrite with a new value
      assert :ok = Cluster.set(config, key, "new_value", set: :only_overwrite)

      # Check the new value
      assert "new_value" = Cluster.get(config, key)

      # Clean up
      Cluster.delete(config, key)
    end

    test "should not overwrite when forbidden", context do
      config = context[:config]
      key = "overwrite_test_key"

      # Set initial value
      assert :ok = Cluster.set(config, key, "initial_value")
      assert "initial_value" = Cluster.get(config, key)

      # Overwrite with a new value
      assert nil == Cluster.set(config, key, "new_value", set: :only_new)

      # Check the new value
      assert "initial_value" = Cluster.get(config, key)

      # Clean up
      Cluster.delete(config, key)
    end

    test "should handle pipelined commands", context do
      config = context[:config]

      key = "pipelined_key"
      value = "value"

      result =
        Cluster.pipeline(
          config,
          [
            ["GET", key],
            ["SET", key, value],
            ["GET", key],
            ["DEL", key]
          ],
          key,
          []
        )

      assert [nil, "OK", "value", 1] = result
      assert nil == Cluster.get(config, key)
    end

    test "should handle generic command to any node", context do
      config = context[:config]

      assert "PONG" = Cluster.command(config, ["PING"], :any, [])
    end

    test "should handle generic command to master node", context do
      config = context[:config]

      result = Cluster.command(config, ["ROLE"], :any, role: :master)

      assert match?(
               ["master", _replication_offset, _replicas = [_, _, _]],
               result
             )
    end

    test "should handle generic command to replica node", context do
      config = context[:config]

      result = Cluster.command(config, ["ROLE"], :any, role: :replica)

      assert match?(
               ["slave", "127.0.0.1", _port, "connected", _replication_offset],
               result
             )
    end

    test "should handle generic pipeline to any node", context do
      config = context[:config]

      assert ["PONG", "PONG", "PONG"] =
               Cluster.pipeline(config, [["PING"], ["PING"], ["PING"]], :any, [])
    end

    test "should handle generic pipeline to master node", context do
      config = context[:config]

      assert ["PONG", ["master", _replication_offset, _replicas = [_, _, _]]] =
               Cluster.pipeline(config, [["PING"], ["ROLE"]], :any, role: :master)
    end

    test "should handle generic pipeline to replica node", context do
      config = context[:config]

      assert ["PONG", ["slave", "127.0.0.1", _port, "connected", _replication_offset]] =
               Cluster.pipeline(config, [["PING"], ["ROLE"]], :any, role: :replica)
    end
  end

  describe "broadcast operations" do
    test "should handle broadcast commands to all nodes", context do
      config = context[:config]

      result =
        config
        |> Cluster.broadcast([~w[DBSIZE], ~w[INFO STATS]])
        |> Enum.sort()

      assert [
               {"127.0.0.1", 6379, _, {:ok, [_, "" <> _]}},
               {"127.0.0.1", 6380, _, {:ok, [_, "" <> _]}},
               {"127.0.0.1", 6381, _, {:ok, [_, "" <> _]}},
               {"127.0.0.1", 6382, _, {:ok, [_, "" <> _]}},
               {"127.0.0.1", 6383, _, {:ok, [_, "" <> _]}},
               {"127.0.0.1", 6384, _, {:ok, [_, "" <> _]}},
               {"127.0.0.1", 6385, _, {:ok, [_, "" <> _]}},
               {"127.0.0.1", 6386, _, {:ok, [_, "" <> _]}},
               {"127.0.0.1", 6387, _, {:ok, [_, "" <> _]}},
               {"127.0.0.1", 6388, _, {:ok, [_, "" <> _]}},
               {"127.0.0.1", 6389, _, {:ok, [_, "" <> _]}},
               {"127.0.0.1", 6390, _, {:ok, [_, "" <> _]}}
             ] = Enum.sort(result)
    end

    test "should handle broadcast commands to master nodes", context do
      config = context[:config]

      result =
        config
        |> Cluster.broadcast([~w[DBSIZE], ~w[INFO STATS]], role: :master)
        |> Enum.sort()

      assert [
               {"127.0.0.1", _, :master, {:ok, [_, "" <> _]}},
               {"127.0.0.1", _, :master, {:ok, [_, "" <> _]}},
               {"127.0.0.1", _, :master, {:ok, [_, "" <> _]}}
             ] = Enum.sort(result)
    end

    test "should handle broadcast commands to replica nodes", context do
      config = context[:config]

      result =
        config
        |> Cluster.broadcast([~w[DBSIZE], ~w[INFO STATS]], role: :replica)
        |> Enum.sort()

      assert [
               {"127.0.0.1", _, :replica, {:ok, [_, "" <> _]}},
               {"127.0.0.1", _, :replica, {:ok, [_, "" <> _]}},
               {"127.0.0.1", _, :replica, {:ok, [_, "" <> _]}},
               {"127.0.0.1", _, :replica, {:ok, [_, "" <> _]}},
               {"127.0.0.1", _, :replica, {:ok, [_, "" <> _]}},
               {"127.0.0.1", _, :replica, {:ok, [_, "" <> _]}},
               {"127.0.0.1", _, :replica, {:ok, [_, "" <> _]}},
               {"127.0.0.1", _, :replica, {:ok, [_, "" <> _]}},
               {"127.0.0.1", _, :replica, {:ok, [_, "" <> _]}}
             ] = Enum.sort(result)
    end

    test "should handle broadcast commands in parallel", context do
      config = context[:config]

      result =
        config
        |> Cluster.broadcast_async([~w[DBSIZE], ~w[INFO STATS]])
        |> Enum.sort()

      assert [
               {"127.0.0.1", 6379, _, {:ok, [_, "" <> _]}},
               {"127.0.0.1", 6380, _, {:ok, [_, "" <> _]}},
               {"127.0.0.1", 6381, _, {:ok, [_, "" <> _]}},
               {"127.0.0.1", 6382, _, {:ok, [_, "" <> _]}},
               {"127.0.0.1", 6383, _, {:ok, [_, "" <> _]}},
               {"127.0.0.1", 6384, _, {:ok, [_, "" <> _]}},
               {"127.0.0.1", 6385, _, {:ok, [_, "" <> _]}},
               {"127.0.0.1", 6386, _, {:ok, [_, "" <> _]}},
               {"127.0.0.1", 6387, _, {:ok, [_, "" <> _]}},
               {"127.0.0.1", 6388, _, {:ok, [_, "" <> _]}},
               {"127.0.0.1", 6389, _, {:ok, [_, "" <> _]}},
               {"127.0.0.1", 6390, _, {:ok, [_, "" <> _]}}
             ] = Enum.sort(result)
    end
  end

  describe "multi-key operations" do
    test "should handle the multi-key operations", context do
      config = context[:config]

      pairs = %{
        "cluster-multi-key-test-1" => "value1",
        "cluster-multi-key-test-2" => "value2",
        "cluster-multi-key-test-3" => "value3"
      }

      assert :ok = Cluster.set_many(config, pairs)
      assert ~w[value1 value2 value3] = Cluster.get_many(config, Map.keys(pairs))
      assert 3 = Cluster.delete_many(config, Map.keys(pairs))
      assert [nil, nil, nil] = Cluster.get_many(config, Map.keys(pairs))
    end

    test "should handle the single-key operations with noreply", context do
      config = context[:config]

      key = "noreply-single-key-test-1"
      value = "value"

      assert :ok = Cluster.set_many(config, %{key => value}, reply: false)
      assert [^value] = Cluster.get_many(config, [key])
      assert :ok = Cluster.delete_many(config, [key], reply: false)
      assert [nil] = Cluster.get_many(config, [key])
    end

    test "should handle the multi-key operations with noreply", context do
      config = context[:config]

      pairs = %{
        "noreply-multi-key-test-1" => "value1",
        "noreply-multi-key-test-2" => "value2",
        "noreply-multi-key-test-3" => "value3"
      }

      assert :ok = Cluster.set_many(config, pairs, reply: false)
      assert ~w[value1 value2 value3] = Cluster.get_many(config, Map.keys(pairs))
      assert :ok = Cluster.delete_many(config, Map.keys(pairs), reply: false)
      assert [nil, nil, nil] = Cluster.get_many(config, Map.keys(pairs))
    end

    test "should handle multi-key operations with an empty list", context do
      config = context[:config]

      assert :ok = Cluster.set_many(config, %{})
      assert :ok = Cluster.set_many(config, [])
      assert [] = Cluster.get_many(config, [])
      assert 0 = Cluster.delete_many(config, [])
    end

    test "should handle multi-key operations with a single item", context do
      config = context[:config]

      key = "single-key"
      value = "value1"

      assert :ok = Cluster.set_many(config, [{key, value}])
      assert ["value1"] = Cluster.get_many(config, [key])
      assert 1 = Cluster.delete_many(config, [key])
      assert [nil] = Cluster.get_many(config, [key])

      assert :ok = Cluster.set_many(config, %{key => value})
      assert ["value1"] = Cluster.get_many(config, [key])
      assert 1 = Cluster.delete_many(config, [key])
      assert [nil] = Cluster.get_many(config, [key])
    end

    test "should handle the multi-key operations with hash tags", context do
      config = context[:config]

      pairs = %{
        "{hashtag}:cluster-multi-key-test-1" => "value1",
        "{hashtag}:cluster-multi-key-test-2" => "value2",
        "{hashtag}:cluster-multi-key-test-3" => "value3"
      }

      assert :ok = Cluster.set_many(config, pairs, compute_hash_tag: true)

      assert ~w[value1 value2 value3] =
               Cluster.get_many(config, Map.keys(pairs), compute_hash_tag: true)

      assert 3 = Cluster.delete_many(config, Map.keys(pairs), compute_hash_tag: true)

      assert [nil, nil, nil] =
               Cluster.get_many(config, Map.keys(pairs), compute_hash_tag: true)
    end

    test "should handle seconds expiration option with multi-key", context do
      config = context[:config]

      pairs = %{
        "second-expiration-multi-key-1" => "value1",
        "second-expiration-multi-key-2" => "value2",
        "second-expiration-multi-key-3" => "value3"
      }

      # Test setting with expiration in seconds
      assert :ok = Cluster.set_many(config, pairs, expire_seconds: 1)
      assert ["value1", "value2", "value3"] = Cluster.get_many(config, Map.keys(pairs))

      # Wait for expiration
      Process.sleep(1100)
      assert [nil, nil, nil] = Cluster.get_many(config, Map.keys(pairs))
    end

    test "should handle milliseconds expiration option with multi-key", context do
      config = context[:config]

      pairs = %{
        "millisecond-expiration-multi-key-1" => "value1",
        "millisecond-expiration-multi-key-2" => "value2",
        "millisecond-expiration-multi-key-3" => "value3"
      }

      # Test setting with expiration in milliseconds
      assert :ok = Cluster.set_many(config, pairs, expire_milliseconds: 100)
      assert ["value1", "value2", "value3"] = Cluster.get_many(config, Map.keys(pairs))

      # Wait for expiration
      Process.sleep(200)
      assert [nil, nil, nil] = Cluster.get_many(config, Map.keys(pairs))
    end

    test "should overwrite when instructed with multi-key", context do
      config = context[:config]

      pairs = %{
        "overwrite_test_multikey1" => "initial",
        "overwrite_test_multikey2" => "initial",
        "overwrite_test_multikey3" => "initial"
      }

      # Fail to set initial value
      assert :ok = Cluster.set_many(config, pairs, set: :only_overwrite)
      assert [nil, nil, nil] = Cluster.get_many(config, Map.keys(pairs))

      # Actually set initial value
      assert :ok = Cluster.set_many(config, pairs)
      assert ~w[initial initial initial] = Cluster.get_many(config, Map.keys(pairs))

      new_pairs = %{
        "overwrite_test_multikey1" => "new",
        "overwrite_test_multikey2" => "new",
        "overwrite_test_multikey3" => "new"
      }

      # Overwrite with a new value
      assert :ok = Cluster.set_many(config, new_pairs, set: :only_overwrite)

      # Check the new value
      assert ~w[new new new] = Cluster.get_many(config, Map.keys(new_pairs))

      # Clean up
      Cluster.delete_many(config, Map.keys(pairs))
    end

    test "should not overwrite when forbidden with multi-key", context do
      config = context[:config]

      pairs = %{
        "write_new_test_multikey1" => "initial",
        "write_new_test_multikey2" => "initial",
        "write_new_test_multikey3" => "initial"
      }

      # Set initial value
      assert :ok = Cluster.set_many(config, pairs, set: :only_new)

      assert ~w[initial initial initial] = Cluster.get_many(config, Map.keys(pairs))

      new_pairs = %{
        "write_new_test_multikey1" => "new",
        "write_new_test_multikey2" => "new",
        "write_new_test_multikey3" => "new"
      }

      # Overwrite with a new value
      assert :ok = Cluster.set_many(config, new_pairs, set: :only_new)

      # Check the new value
      assert ~w[initial initial initial] = Cluster.get_many(config, Map.keys(pairs))

      # Clean up
      Cluster.delete_many(config, Map.keys(pairs))
    end

    test "should only send one command for duplicated keys", context do
      config = context[:config]

      {:ok, monitors} =
        RedisCluster.Monitor.monitor_cluster_nodes(config, role: :master, max_commands: 50)

      key = "duplicated-key"
      value = "value1"

      # Give monitors time to start up.
      Process.sleep(100)

      # Set the value and fetch it multiple times.
      Cluster.set_many(config, [{key, value}, {key, value}, {key, value}])
      assert [^value, ^value, ^value] = Cluster.get_many(config, [key, key, key])
      Cluster.delete_many(config, [key, key, key])

      # Get all commands from all monitors
      all_commands =
        for %{monitor_pid: monitor_pid} <- monitors do
          RedisCluster.Monitor.get_commands(monitor_pid)
        end
        |> List.flatten()

      get_commands = Enum.filter(all_commands, &(&1.command =~ ~r/GET.*#{key}/))
      set_commands = Enum.filter(all_commands, &(&1.command =~ ~r/SET.*#{key}/))
      del_commands = Enum.filter(all_commands, &(&1.command =~ ~r/DEL.*#{key}/))

      # Should only have 1 GET command for the duplicated key
      assert 1 = length(get_commands)

      # Should only have 1 SET command for the duplicated key
      assert 1 = length(set_commands)

      # Should only have 1 DEL command for the duplicated key
      assert 1 = length(del_commands)

      # Clean up monitors and delete the key.
      for %{monitor_pid: monitor_pid} <- monitors do
        RedisCluster.Monitor.stop(monitor_pid)
      end
    end
  end

  describe "async multi-key operations" do
    test "should handle multi-key operations in parallel", context do
      config = context[:config]

      pairs = %{
        "parallel-multi-key-1" => "value1",
        "parallel-multi-key-2" => "value2",
        "parallel-multi-key-3" => "value3"
      }

      assert :ok = Cluster.set_many_async(config, pairs)
      assert ~w[value1 value2 value3] = Cluster.get_many_async(config, Map.keys(pairs))
      assert 3 = Cluster.delete_many_async(config, Map.keys(pairs))
      assert [nil, nil, nil] = Cluster.get_many_async(config, Map.keys(pairs))
    end

    test "should handle overwrite only multi-key SET in parallel", context do
      config = context[:config]

      pairs = %{
        "parallel-multi-key-overwrite-only-1" => "value1",
        "parallel-multi-key-overwrite-only-2" => "value2",
        "parallel-multi-key-overwrite-only-3" => "value3"
      }

      assert :ok = Cluster.set_many_async(config, pairs, set: :only_overwrite)
      assert [nil, nil, nil] = Cluster.get_many_async(config, Map.keys(pairs))

      assert :ok = Cluster.set_many_async(config, pairs)
      assert ~w[value1 value2 value3] = Cluster.get_many_async(config, Map.keys(pairs))

      assert 3 = Cluster.delete_many_async(config, Map.keys(pairs))
      assert [nil, nil, nil] = Cluster.get_many_async(config, Map.keys(pairs))
    end

    test "should handle milliseconds expiration option with multi-key in parallel", context do
      config = context[:config]

      pairs = %{
        "parallel-multi-key-millisecond-expiration-1" => "value1",
        "parallel-multi-key-millisecond-expiration-2" => "value2",
        "parallel-multi-key-millisecond-expiration-3" => "value3"
      }

      assert :ok = Cluster.set_many_async(config, pairs, expire_milliseconds: 100)
      assert ~w[value1 value2 value3] = Cluster.get_many_async(config, Map.keys(pairs))

      Process.sleep(200)
      assert [nil, nil, nil] = Cluster.get_many_async(config, Map.keys(pairs))
    end

    test "should handle seconds expiration option with multi-key in parallel", context do
      config = context[:config]

      pairs = %{
        "parallel-multi-key-second-expiration-1" => "value1",
        "parallel-multi-key-second-expiration-2" => "value2",
        "parallel-multi-key-second-expiration-3" => "value3"
      }

      assert :ok = Cluster.set_many_async(config, pairs, expire_seconds: 1)
      assert ~w[value1 value2 value3] = Cluster.get_many_async(config, Map.keys(pairs))

      Process.sleep(1100)
      assert [nil, nil, nil] = Cluster.get_many_async(config, Map.keys(pairs))
    end

    test "should handle only new multi-key SET in parallel", context do
      config = context[:config]

      pairs = %{
        "parallel-multi-key-only-new-1" => "value1",
        "parallel-multi-key-only-new-2" => "value2",
        "parallel-multi-key-only-new-3" => "value3"
      }

      assert :ok = Cluster.set_many_async(config, pairs, set: :only_new)
      assert ~w[value1 value2 value3] = Cluster.get_many_async(config, Map.keys(pairs))

      other_pairs = Map.new(pairs, fn {k, _v} -> {k, "FAIL"} end)

      assert :ok = Cluster.set_many_async(config, other_pairs, set: :only_new)
      assert ~w[value1 value2 value3] = Cluster.get_many_async(config, Map.keys(pairs))

      assert 3 = Cluster.delete_many_async(config, Map.keys(pairs))
      assert [nil, nil, nil] = Cluster.get_many_async(config, Map.keys(pairs))
    end

    test "should handle multi-key operations in parallel with noreply", context do
      config = context[:config]

      pairs = %{
        "parallel-multi-key-noreply-1" => "value1",
        "parallel-multi-key-noreply-2" => "value2",
        "parallel-multi-key-noreply-3" => "value3"
      }

      assert :ok = Cluster.set_many_async(config, pairs, reply: false)
      assert ~w[value1 value2 value3] = Cluster.get_many_async(config, Map.keys(pairs))
      assert :ok = Cluster.delete_many_async(config, Map.keys(pairs), reply: false)
      assert [nil, nil, nil] = Cluster.get_many_async(config, Map.keys(pairs))
    end

    test "should handle multi-key operations in parallel with hash tags and noreply", context do
      config = context[:config]

      pairs = %{
        "parallel-multi-key-noreply-1{hashtag}" => "value1",
        "parallel-multi-key-noreply-2{hashtag}" => "value2",
        "parallel-multi-key-noreply-3{hashtag}" => "value3"
      }

      assert :ok = Cluster.set_many_async(config, pairs, reply: false, compute_hash_tag: true)

      assert ~w[value1 value2 value3] =
               Cluster.get_many_async(config, Map.keys(pairs), compute_hash_tag: true)

      assert :ok =
               Cluster.delete_many_async(config, Map.keys(pairs),
                 reply: false,
                 compute_hash_tag: true
               )

      assert [nil, nil, nil] =
               Cluster.get_many_async(config, Map.keys(pairs), compute_hash_tag: true)
    end

    test "should handle multi-key operations in parallel with hash tags", context do
      config = context[:config]

      pairs = %{
        "parallel-multi-key-1{hashtag}" => "value1",
        "parallel-multi-key-2{hashtag}" => "value2",
        "parallel-multi-key-3{hashtag}" => "value3"
      }

      assert :ok = Cluster.set_many_async(config, pairs, compute_hash_tag: true)

      assert ~w[value1 value2 value3] =
               Cluster.get_many_async(config, Map.keys(pairs), compute_hash_tag: true)

      assert 3 = Cluster.delete_many_async(config, Map.keys(pairs), compute_hash_tag: true)

      assert [nil, nil, nil] =
               Cluster.get_many_async(config, Map.keys(pairs), compute_hash_tag: true)
    end

    test "should handle multi-key operations in parallel with empty list", context do
      config = context[:config]

      assert :ok = Cluster.set_many_async(config, %{})
      assert :ok = Cluster.set_many_async(config, [])
      assert [] = Cluster.get_many_async(config, [])
      assert 0 = Cluster.delete_many_async(config, [])
    end

    test "should handle multi-key operations in parallel with a single item", context do
      config = context[:config]

      pairs = %{"parallel-multi-key-1" => "value1"}

      assert :ok = Cluster.set_many_async(config, pairs)
      assert :ok = Cluster.set_many_async(config, Enum.to_list(pairs))
      assert ~w[value1] = Cluster.get_many_async(config, Map.keys(pairs))
      assert 1 = Cluster.delete_many_async(config, Map.keys(pairs))
      assert [nil] = Cluster.get_many_async(config, Map.keys(pairs))
    end

    test "should get ok when setting with no reply", context do
      config = context[:config]

      key = "set-noreply-key"
      value = "value"

      assert :ok = Cluster.set_many_async(config, %{key => value}, reply: false)
      assert value == Cluster.get(config, key)
    end

    test "should get ok when deleting with no reply", context do
      config = context[:config]

      key = "delete-noreply-key"
      value = "value"

      assert :ok = Cluster.set(config, key, value)
      assert value == Cluster.get(config, key)
      assert :ok = Cluster.delete_many_async(config, [key], reply: false)
      assert nil == Cluster.get(config, key)
    end

    test "should only send one command for duplicated keys in parallel", context do
      config = context[:config]

      {:ok, monitors} =
        RedisCluster.Monitor.monitor_cluster_nodes(config, role: :master, max_commands: 50)

      key = "duplicated-key-parallel"
      value = "value1"

      # Give monitors time to start up.
      Process.sleep(100)

      # Set the value and fetch it multiple times.
      Cluster.set_many_async(config, [{key, value}, {key, value}, {key, value}])

      assert [^value, ^value, ^value] = Cluster.get_many_async(config, [key, key, key])

      Cluster.delete_many_async(config, [key, key, key])

      # Get all commands from all monitors
      all_commands =
        for %{monitor_pid: monitor_pid} <- monitors do
          RedisCluster.Monitor.get_commands(monitor_pid)
        end
        |> List.flatten()

      get_commands = Enum.filter(all_commands, &(&1.command =~ ~r/GET.*#{key}/))
      set_commands = Enum.filter(all_commands, &(&1.command =~ ~r/SET.*#{key}/))
      del_commands = Enum.filter(all_commands, &(&1.command =~ ~r/DEL.*#{key}/))

      # Should only have 1 GET/MGET command for the duplicated key
      assert 1 = length(get_commands)

      # Should only have 1 SET command for the duplicated key
      assert 1 = length(set_commands)

      # Should only have 1 DEL command for the duplicated key
      assert 1 = length(del_commands)

      # Clean up monitors and delete the key.
      for %{monitor_pid: monitor_pid} <- monitors do
        RedisCluster.Monitor.stop(monitor_pid)
      end
    end
  end

  describe "hash tag operations" do
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
        assert :ok = Cluster.set(config, key, value, compute_hash_tag: true)
      end

      # Try a multi-key operation which should work since all keys hash to the same slot
      cmd = ["MGET" | keys]

      assert ["30", "john@example.com", "John"] ==
               Cluster.command(config, cmd, List.first(keys), compute_hash_tag: true)

      # Clean up
      for key <- keys do
        Cluster.delete(config, key, compute_hash_tag: true)
      end
    end

    test "should avoid cross-slot error with hash tags", context do
      config = context[:config]

      # Keys that will hash to different slots
      different_slot_keys = ["key1", "key2"]

      for {key, value} <- Enum.zip(different_slot_keys, ["value1", "value2"]) do
        assert :ok = Cluster.set(config, key, value)
      end

      # MGET across slots should return an error
      cmd = ["MGET" | different_slot_keys]

      result =
        Cluster.command(config, cmd, List.first(different_slot_keys), compute_hash_tag: true)

      assert match?({:error, _}, result)

      # Clean up
      for key <- different_slot_keys do
        Cluster.delete(config, key)
      end
    end
  end
end
