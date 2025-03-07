defmodule RedisClusterTest do
  use ExUnit.Case
  doctest RedisCluster

  test "greets the world" do
    assert RedisCluster.hello() == :world
  end
end
