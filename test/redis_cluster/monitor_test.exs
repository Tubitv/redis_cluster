defmodule MonitorTest do
  # Async is false to avoid monitoring commands from other tests
  use ExUnit.Case, async: false

  alias RedisCluster.Cluster
  alias RedisCluster.Monitor
  alias RedisCluster.Monitor.Message

  setup_all do
    config = %RedisCluster.Configuration{
      host: "127.0.0.1",
      port: 6379,
      name: :test_monitor_cluster,
      registry: :test_monitor_registry,
      cluster: :test_monitor_cluster_sup,
      pool: :test_monitor_pool,
      shard_discovery: :test_monitor_shard_discovery,
      pool_size: 1
    }

    Cluster.start_link(config)

    wait_for_cluster_to_be_ready(config)

    {:ok, config: config}
  end

  describe "Monitor" do
    test "can start and stop monitoring" do
      # Start a monitor process
      {:ok, monitor_pid} =
        Monitor.start_link(host: "127.0.0.1", port: 6379, max_commands: 10)

      Process.sleep(100)

      # Generate some Redis activity with direct Redix connection
      {:ok, conn} = Redix.start_link(host: "127.0.0.1", port: 6379)
      Redix.command(conn, ["PING"])
      Redix.command(conn, ["ECHO", "test"])

      # Wait for activity and then get commands
      Process.sleep(500)

      # Get collected commands
      commands = monitor_pid |> Monitor.get_commands() |> Enum.map(& &1.command)

      assert "\"PING\"" in commands
      assert "\"ECHO\" \"test\"" in commands

      # Clear commands
      :ok = Monitor.clear_commands(monitor_pid)
      assert [] = Monitor.get_commands(monitor_pid)

      # Stop the monitor process
      :ok = Monitor.stop(monitor_pid)
      GenServer.stop(conn)
    end

    test "can monitor a single node via cluster helper" do
      # Start monitoring via cluster helper
      {:ok, monitor_pid} = Monitor.monitor_node("127.0.0.1", 6379, max_commands: 10)

      # Generate activity with direct connection to ensure it goes to the right node
      {:ok, conn} = Redix.start_link(host: "127.0.0.1", port: 6379)
      Process.sleep(500)
      Redix.command(conn, ["SET", "cluster_helper_test", "value"])

      # Wait for activity and get commands
      Process.sleep(1000)

      commands = Monitor.get_commands(monitor_pid)

      # Check that we got a SET command
      set_commands = Enum.filter(commands, &(&1.command =~ ~r/SET.*cluster_helper_test/i))

      assert length(set_commands) >= 1

      # Clean up
      Monitor.stop(monitor_pid)
      GenServer.stop(conn)
    end

    test "can monitor multiple cluster nodes", context do
      config = context[:config]

      # Start monitoring all master nodes
      {:ok, monitors} =
        Monitor.monitor_cluster_nodes(config, role: :master, max_commands: 10)

      # Should have at least one monitor
      assert length(monitors) > 0

      # Wait longer for monitors to be fully connected
      Process.sleep(300)

      # Ping all the nodes
      Cluster.broadcast(config, [~w[ECHO broadcast_test]])

      commands =
        monitors
        |> Enum.flat_map(fn {_host, _port, _role, monitor_pid} ->
          Monitor.get_commands(monitor_pid)
        end)
        |> Enum.filter(&(&1.command =~ "broadcast_test"))

      # Each monitored node should have received a command
      assert length(commands) == length(monitors)

      # Clean up all monitors
      for {_host, _port, _role, monitor_pid} <- monitors do
        Monitor.stop(monitor_pid)
      end
    end

    @tag :current
    test "should limit the number of commands", context do
      config = context[:config]

      port = 6380
      max_commands = 3
      {:ok, monitor_pid} = Monitor.monitor_node("127.0.0.1", port, max_commands: max_commands)

      # Wait longer for monitor to be fully connected
      Process.sleep(200)

      # Find the slots the node handles.
      [{start, stop}] =
        for {_mod, start, stop, _role, _host, ^port} <- RedisCluster.HashSlots.all_slots(config) do
          {start, stop}
        end

      # Find a key that hashes to the same slot as the node.
      # This will be used as a hash tag.
      hash_tag =
        Enum.find(1..1000, fn n ->
          RedisCluster.Key.hash_slot("#{n}") in start..stop
        end)

      # Sends several commands to the same node.
      for n <- 1..10 do
        Cluster.set(config, "{#{hash_tag}}:limit_test#{n}", "value#{n}", compute_hash_tag: true)

        Process.sleep(1000)
      end

      # Wait for monitor to process
      Process.sleep(1000)

      # Should only have 3 commands due to max_commands limit
      commands = Monitor.get_commands(monitor_pid)
      assert length(commands) == max_commands

      # Should have the latest commands
      assert Enum.at(commands, -1).command =~ ~r/SET.*limit_test10/i
      assert Enum.at(commands, -2).command =~ ~r/SET.*limit_test9/i
      assert Enum.at(commands, -3).command =~ ~r/SET.*limit_test8/i

      # Clean up
      Monitor.stop(monitor_pid)
    end
  end

  describe "message format" do
    test "messages should contain expected fields" do
      {:ok, monitor_pid} = Monitor.monitor_node("127.0.0.1", 6380, max_commands: 10)
      {:ok, conn} = Redix.start_link(host: "127.0.0.1", port: 6380)

      # Wait longer for monitor to be fully connected
      Process.sleep(200)

      Redix.command(conn, ["SET", "format_test", "test_value"])

      # Extra wait for monitor to process
      Process.sleep(500)

      # Use retry logic to wait for commands to appear
      commands = Monitor.get_commands(monitor_pid)
      set_commands = Enum.filter(commands, &(&1.command =~ "format_test"))
      assert length(set_commands) >= 1

      # Verify message format
      [message | _] = commands
      assert %Message{host: host, port: port, command: msg, timestamp: ts} = message
      assert host == "127.0.0.1"
      assert port == 6380
      assert is_binary(msg)
      assert is_float(ts)
      assert ts > 0

      # The message should contain information about the SET command
      assert String.contains?(msg, "SET") or String.contains?(msg, "set")

      # Clean up
      Monitor.stop(monitor_pid)
      GenServer.stop(conn)
    end
  end

  describe "error handling" do
    test "handles connection failures gracefully" do
      # Try to connect to non-existent Redis instance
      {:ok, monitor_pid} =
        Monitor.start_link(
          host: "127.0.0.1",
          # Non-existent port
          port: 9999,
          max_commands: 10
        )

      # Give it time to attempt connection
      Process.sleep(2000)

      # Should have no commands since connection failed
      commands = Monitor.get_commands(monitor_pid)
      assert commands == []

      # Should still be able to clear commands
      :ok = Monitor.clear_commands(monitor_pid)

      # Clean up
      Monitor.stop(monitor_pid)
    end
  end

  defp wait_for_cluster_to_be_ready(config, attempts \\ 10)

  defp wait_for_cluster_to_be_ready(_config, 0) do
    :error
  end

  defp wait_for_cluster_to_be_ready(config, attempts) do
    if RedisCluster.HashSlots.all_slots(config) == [] do
      Process.sleep(500)
      wait_for_cluster_to_be_ready(config, attempts - 1)
    else
      :ok
    end
  end
end
