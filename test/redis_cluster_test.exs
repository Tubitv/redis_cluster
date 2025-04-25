defmodule TestRedis do
  use RedisCluster,
    otp_app: :none,
    host: "localhost",
    port: 6379,
    pool_size: 3
end

defmodule RedisClusterTest do
  use ExUnit.Case, async: true

  doctest RedisCluster.Lock
  doctest RedisCluster.Key
  doctest RedisCluster.Table

  setup_all do
    case TestRedis.start_link([]) do
      {:ok, pid} ->
        pid

      {:error, {:already_started, pid}} ->
        pid
    end

    # Wait for cluster discovery
    Process.sleep(2000)

    :ok
  end

  test "should handle basic operations" do
    key = "basic_ops_test_key"

    assert :ok = TestRedis.set(key, "test_value")
    assert "test_value" = TestRedis.get(key)
    assert 1 = TestRedis.delete(key)
    assert nil == TestRedis.get(key)
  end

  test "should handle the multi-key operations" do
    pairs = %{
      "multi-key-test-1" => "value1",
      "multi-key-test-2" => "value2",
      "multi-key-test-3" => "value3"
    }

    assert :ok = TestRedis.set_many(pairs)
    assert ~w[value1 value2 value3] = TestRedis.get_many(Map.keys(pairs))
    assert :ok = TestRedis.delete_many(Map.keys(pairs))
    assert [nil, nil, nil] = TestRedis.get_many(Map.keys(pairs))
  end
end
