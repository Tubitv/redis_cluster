defmodule RedisClusterTest do
  use ExUnit.Case, async: true

  doctest RedisCluster.Lock
  doctest RedisCluster.Key
end
