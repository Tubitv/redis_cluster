# defmodule RedisCluster.ShardDiscoveryTest do
#   use ExUnit.Case, async: false
# 
#   alias RedisCluster.Cluster
#   alias RedisCluster.ShardDiscovery
# 
#   setup_all do
#     config =
#       RedisCluster.Configuration.from_app_env(
#         [
#           otp_app: :none,
#           host: "localhost",
#           port: 6379,
#           pool_size: 3
#         ],
#         RedisCluster.DiscoveryTest
#       )
# 
#     case Cluster.start_link(config) do
#       {:ok, pid} ->
#         pid
# 
#       {:error, {:already_started, pid}} ->
#         pid
#     end
# 
#     # Wait for cluster discovery
#     Process.sleep(2000)
# 
#     {:ok, config: config}
#   end
# 
#   test "should not have duplicated nodes after manually rediscovering", context do
#     config = context[:config]
# 
#     check_slots(config)
# 
#     ShardDiscovery.rediscover_shards(config)
# 
#     # Wait for cluster discovery
#     Process.sleep(2000)
# 
#     check_slots(config)
#   end
# 
#   test "should not have duplicated nodes after triggering rediscovery", context do
#     config = context[:config]
# 
#     check_slots(config)
# 
#     # Sending a command to the wrong node to trigger rediscovery.
#     assert {:error, %Redix.Error{message: "MOVED" <> _}} =
#              Cluster.command(config, ~w[SET key value], key: "wrong")
# 
#     # Wait for cluster discovery
#     Process.sleep(2000)
# 
#     check_slots(config)
#   end
# 
#   defp check_slots(config) do
#     slots = config |> RedisCluster.HashSlots.all_slots() |> Enum.sort()
# 
#     assert [
#              {RedisCluster.HashSlots, 0, 5460, :master, "127.0.0.1", _},
#              {RedisCluster.HashSlots, 0, 5460, :replica, "127.0.0.1", _},
#              {RedisCluster.HashSlots, 0, 5460, :replica, "127.0.0.1", _},
#              {RedisCluster.HashSlots, 0, 5460, :replica, "127.0.0.1", _},
#              {RedisCluster.HashSlots, 5461, 10922, :master, "127.0.0.1", _},
#              {RedisCluster.HashSlots, 5461, 10922, :replica, "127.0.0.1", _},
#              {RedisCluster.HashSlots, 5461, 10922, :replica, "127.0.0.1", _},
#              {RedisCluster.HashSlots, 5461, 10922, :replica, "127.0.0.1", _},
#              {RedisCluster.HashSlots, 10923, 16383, :master, "127.0.0.1", _},
#              {RedisCluster.HashSlots, 10923, 16383, :replica, "127.0.0.1", _},
#              {RedisCluster.HashSlots, 10923, 16383, :replica, "127.0.0.1", _},
#              {RedisCluster.HashSlots, 10923, 16383, :replica, "127.0.0.1", _}
#            ] = slots
#   end
# end
