defmodule KinoRedisClusterTest do
  use ExUnit.Case

  test "application starts successfully" do
    # Test that the application starts without errors
    assert Process.whereis(KinoRedisCluster.Supervisor) != nil
  end
end
