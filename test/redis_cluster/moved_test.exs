defmodule RedisCluster.MovedTest do
  # Global Mox mode: rediscovery spawns a fresh connection process and a Task,
  # both of which need to call into the mock, so this can't stay in Mox's
  # default per-process (:private) mode.
  use ExUnit.Case, async: false

  @moduletag :moved

  import Mox

  setup :set_mox_global
  setup :verify_on_exit!

  setup do
    config = %RedisCluster.Configuration{
      host: "192.168.1.100",
      port: 6379,
      name: :test_moved_cluster,
      redis_module: MockRedis,
      registry: :test_moved_registry,
      cluster: :test_moved_cluster_sup,
      pool: :test_moved_pool,
      shard_discovery: :test_moved_shard_discovery,
      pool_size: 1
    }

    {:ok, config: config}
  end

  test "GET command handles MOVED redirect by rediscovering and retrying", %{config: config} do
    {:ok, topology} = Agent.start_link(fn -> cluster_shards_reply("192.168.1.100", 6379) end)

    stub(MockRedis, :start_link, &mock_start_link/1)

    stub(MockRedis, :command, fn _conn, ~w[CLUSTER SHARDS] -> {:ok, Agent.get(topology, & &1)} end)

    start_cluster_infrastructure(config)
    wait_for_slots(config, "192.168.1.100", 6379)

    Agent.update(topology, fn _ -> cluster_shards_reply("192.168.1.101", 6379) end)

    MockRedis
    |> expect(:pipeline, fn _conn, [["GET", _key]] ->
      {:ok, [%Redix.Error{message: "MOVED 12345 192.168.1.101:6379"}]}
    end)
    |> stub(:pipeline, fn _conn, [["GET", _key]] -> {:ok, ["moved_value"]} end)

    assert RedisCluster.Cluster.get(config, "test_key") == "moved_value"

    assert [{RedisCluster.HashSlots, 0, 16383, :master, "192.168.1.101", 6379}] =
             RedisCluster.HashSlots.all_slots(config)
  end

  test "a non-redirect Redis error is returned to the caller without rediscovering", %{
    config: config
  } do
    {:ok, topology} = Agent.start_link(fn -> cluster_shards_reply("192.168.1.100", 6379) end)

    stub(MockRedis, :start_link, &mock_start_link/1)

    stub(MockRedis, :command, fn _conn, ~w[CLUSTER SHARDS] -> {:ok, Agent.get(topology, & &1)} end)

    start_cluster_infrastructure(config)
    wait_for_slots(config, "192.168.1.100", 6379)

    expect(MockRedis, :pipeline, fn _conn, [["GET", _key]] ->
      {:ok,
       [
         %Redix.Error{
           message: "WRONGTYPE Operation against a key holding the wrong kind of value"
         }
       ]}
    end)

    assert {:error, %Redix.Error{message: "WRONGTYPE" <> _}} =
             RedisCluster.Cluster.get(config, "test_key")

    assert [{RedisCluster.HashSlots, 0, 16383, :master, "192.168.1.100", 6379}] =
             RedisCluster.HashSlots.all_slots(config)
  end

  ## Helpers

  # Pool.start_pool passes a `:name` (a Registry `:via` tuple) so connections
  # can be looked up later; the mock must actually register under it.
  defp mock_start_link(opts) do
    case Keyword.get(opts, :name) do
      nil -> Agent.start_link(fn -> nil end)
      name -> Agent.start_link(fn -> nil end, name: name)
    end
  end

  defp start_cluster_infrastructure(config) do
    start_supervised!({Registry, keys: :unique, name: config.registry})
    start_supervised!({DynamicSupervisor, name: config.pool, strategy: :one_for_one})
    start_supervised!({RedisCluster.ShardDiscovery, config})
  end

  defp wait_for_slots(config, host, port, attempts \\ 40) do
    case RedisCluster.HashSlots.all_slots(config) do
      [{RedisCluster.HashSlots, _lo, _hi, :master, ^host, ^port}] ->
        :ok

      _ when attempts > 0 ->
        Process.sleep(5)
        wait_for_slots(config, host, port, attempts - 1)

      other ->
        flunk("Timed out waiting for slots to point at #{host}:#{port}, got: #{inspect(other)}")
    end
  end

  defp cluster_shards_reply(ip, port) do
    [
      [
        "slots",
        [0, 16383],
        "nodes",
        [
          [
            "id",
            "node1",
            "port",
            port,
            "ip",
            ip,
            "role",
            "master",
            "replication-offset",
            0,
            "health",
            "online"
          ]
        ]
      ]
    ]
  end
end
