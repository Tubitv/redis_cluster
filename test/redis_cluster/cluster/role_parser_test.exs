defmodule RedisCluster.Cluster.RoleParserTest do
  use ExUnit.Case, async: true

  alias RedisCluster.Cluster.RoleParser
  alias RedisCluster.Cluster.NodeInfo

  @moduletag :parser

  setup do
    config = %RedisCluster.Configuration{
      host: "localhost",
      port: 6379,
      name: :parser_test,
      registry: :parser_test_registry,
      cluster: :parser_test_cluster,
      pool: :parser_test_pool,
      shard_discovery: :parser_test_shard_discovery,
      pool_size: 10
    }

    {:ok, config: config}
  end

  test "should parse the output of the ROLE command from master with no replicas", context do
    config = context[:config]

    result = RoleParser.parse(["master", "6617", []], config)

    assert result ==
             {:full,
              %NodeInfo{
                id: "",
                slots: [{RedisCluster.HashSlots, 0, 16383, :master, "localhost", 6379}],
                host: "localhost",
                port: 6379,
                role: :master,
                health: :online
              }, []}
  end

  test "should parse the output of the ROLE command from master with one replica", context do
    config = context[:config]

    result = RoleParser.parse(["master", "1829", [["::1", "6380", "1829"]]], config)

    master = %NodeInfo{
      id: "",
      slots: [{RedisCluster.HashSlots, 0, 16383, :master, "localhost", 6379}],
      host: "localhost",
      port: 6379,
      role: :master,
      health: :online
    }

    replica = %RedisCluster.Cluster.NodeInfo{
      id: "",
      slots: [{RedisCluster.HashSlots, 0, 16383, :replica, "localhost", 6380}],
      host: "localhost",
      port: 6380,
      role: :replica,
      health: :online
    }

    assert result == {:full, master, [replica]}
  end

  test "should parse the output of the ROLE command from replica", context do
    config = context[:config]

    result = RoleParser.parse(["slave", "localhost", "6379", "connected", "1829"], config)

    assert master = %RedisCluster.Cluster.NodeInfo{
             id: "",
             slots: [{RedisCluster.HashSlots, 0, 16383, :master, "localhost", 6379}],
             host: "localhost",
             port: 6379,
             role: :master,
             health: :online
           }

    assert replica = %RedisCluster.Cluster.NodeInfo{
             id: "",
             slots: [{RedisCluster.HashSlots, 0, 16383, :replica, "localhost", 6379}],
             host: "localhost",
             port: 6379,
             role: :replica,
             health: :online
           }

    assert result == {:partial, master, [replica]}
  end

  test "should fail to parse the output of the ROLE command from sentinel", context do
    config = context[:config]

    assert_raise RuntimeError, fn ->
      RoleParser.parse(["sentinel", []], config)
    end
  end
end
