defmodule RedisCluster.AskTest do
  use ExUnit.Case, async: true

  @moduletag :ask

  setup :verify_on_exit!

  import Mox

  setup do
    # Create a simple configuration for testing
    config = %RedisCluster.Configuration{
      host: "192.168.1.100",
      port: 6379,
      name: :test_ask_cluster,
      redis_module: MockRedis,
      registry: :test_ask_registry,
      cluster: :test_cluster_sup,
      pool: :test_pool,
      shard_discovery: :test_shard_discovery,
      pool_size: 1
    }

    {:ok, config: config}
  end

  test "SET command handles ASK redirect properly", %{config: config} do
    try do
      setup_cluster_infrastructure(config)

      # Set up Mox expectations for the ASK redirect flow
      MockRedis
      |> expect(:pipeline, fn _conn, [["SET", _key, _value]] ->
        # First call returns an ASK redirect
        {:error, %Redix.Error{message: "ASK 12345 192.168.1.100:6379"}}
      end)
      |> expect(:pipeline, fn _conn, [["ASKING"], ["SET", _key, _value]] ->
        {:ok, ["OK", "OK"]}
      end)

      # Actually call the Cluster.set function - this tests the real application code
      result = RedisCluster.Cluster.set(config, "test_key", "test_value")

      # Verify that the ASK redirect was handled and we got the expected result
      assert result == :ok
    after
      cleanup_cluster_infrastructure(config)
    end
  end

  test "GET command handles ASK redirect properly", %{config: config} do
    try do
      setup_cluster_infrastructure(config)

      MockRedis
      |> expect(:pipeline, fn _conn, [["GET", _key]] ->
        {:error, %Redix.Error{message: "ASK 12345 192.168.1.100:6379"}}
      end)
      |> expect(:pipeline, fn _conn, [["ASKING"], ["GET", _key]] ->
        {:ok, ["OK", "test_value"]}
      end)

      result = RedisCluster.Cluster.get(config, "test_key")
      assert result == "test_value"
    after
      cleanup_cluster_infrastructure(config)
    end
  end

  test "DELETE command handles ASK redirect properly", %{config: config} do
    try do
      setup_cluster_infrastructure(config)

      MockRedis
      |> expect(:pipeline, fn _conn, [["DEL", _key]] ->
        {:error, %Redix.Error{message: "ASK 12345 192.168.1.100:6379"}}
      end)
      |> expect(:pipeline, fn _conn, [["ASKING"], ["DEL", _key]] ->
        {:ok, ["OK", 1]}
      end)

      result = RedisCluster.Cluster.delete(config, "test_key")
      assert result == 1
    after
      cleanup_cluster_infrastructure(config)
    end
  end

  test "pipeline command handles ASK redirect properly", %{config: config} do
    try do
      setup_cluster_infrastructure(config)

      MockRedis
      |> expect(:pipeline, fn _conn, [["GET", _key1], ["SET", _key2, _value2]] ->
        {:error, %Redix.Error{message: "ASK 12345 192.168.1.100:6379"}}
      end)
      |> expect(:pipeline, fn _conn, [["ASKING"], ["GET", _key1], ["SET", _key2, _value2]] ->
        {:ok, ["OK", "value1", "OK"]}
      end)

      result =
        RedisCluster.Cluster.pipeline(config, [["GET", "key1"], ["SET", "key2", "value2"]],
          key: "key1"
        )

      assert result == ["value1", "OK"]
    after
      cleanup_cluster_infrastructure(config)
    end
  end

  test "pipeline command handles ASK port-only redirect properly", %{config: config} do
    try do
      setup_cluster_infrastructure(config)

      MockRedis
      |> expect(:pipeline, fn _conn, [["GET", _key1], ["SET", _key2, _value2]] ->
        {:error, %Redix.Error{message: "ASK 12345 :6380"}}
      end)
      |> expect(:pipeline, fn _conn, [["ASKING"], ["GET", _key1], ["SET", _key2, _value2]] ->
        {:ok, ["OK", "value1", "OK"]}
      end)

      result =
        RedisCluster.Cluster.pipeline(config, [["GET", "key1"], ["SET", "key2", "value2"]],
          key: "key1"
        )

      assert result == ["value1", "OK"]
    after
      cleanup_cluster_infrastructure(config)
    end
  end

  # Helper functions to set up the minimal infrastructure needed for testing

  defp setup_cluster_infrastructure(config) do
    RedisCluster.HashSlots.create_table(config)

    # Add slot information covering the full hash space
    slot_id_1 = RedisCluster.HashSlots.slot_id(0, 8191, :master, "192.168.1.100", 6379)
    slot_id_2 = RedisCluster.HashSlots.slot_id(8192, 16383, :master, "192.168.1.101", 6379)

    RedisCluster.HashSlots.add_slots(config, [slot_id_1, slot_id_2])

    # Create a Registry to mock the Pool's connection lookup
    start_supervised!({Registry, keys: :unique, name: config.registry})

    # Register our test process as a connection for both potential hosts/ports
    Registry.register(config.registry, {"192.168.1.100", 6379, 0}, nil)
    Registry.register(config.registry, {"192.168.1.101", 6379, 0}, nil)
  end

  defp cleanup_cluster_infrastructure(config) do
    RedisCluster.HashSlots.delete(config)
    # Registry will be stopped automatically by ExUnit
  end
end
