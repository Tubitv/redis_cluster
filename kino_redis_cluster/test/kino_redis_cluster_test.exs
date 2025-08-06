defmodule KinoRedisClusterTest do
  use ExUnit.Case
  doctest KinoRedisCluster

  test "greets the world" do
    assert KinoRedisCluster.hello() == :world
  end
end
