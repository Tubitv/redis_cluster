defmodule RedisCluster.TelemetryTest do
  # Async is false to avoid picking up telemetry events from other tests
  use ExUnit.Case, async: false

  alias RedisCluster.Telemetry

  # Test handler module to avoid anonymous function warnings
  defmodule TestHandler do
    def handle_event(event, measurements, metadata, test_pid) do
      send(test_pid, {:telemetry_event, event, measurements, metadata})
    end
  end

  describe "telemetry events" do
    test "execute_command/3 emits start and stop events" do
      # Setup telemetry handler to capture events
      ref = make_ref()

      :telemetry.attach_many(
        ref,
        [
          [:redis_cluster, :command, :start],
          [:redis_cluster, :command, :stop]
        ],
        &TestHandler.handle_event/4,
        self()
      )

      # Execute a command with telemetry
      command = ["GET", "test_key"]

      metadata = %{
        config_name: :test_config,
        key: "test_key",
        role: :master,
        slot: 1234
      }

      result =
        Telemetry.execute_command(command, metadata, fn ->
          # Simulate some work
          Process.sleep(10)
          "test_value"
        end)

      # Verify the result
      assert result == "test_value"

      # Verify start event was emitted
      assert_receive {:telemetry_event, [:redis_cluster, :command, :start], start_measurements,
                      start_metadata}

      assert Map.has_key?(start_measurements, :system_time)
      assert start_metadata.config_name == :test_config
      assert start_metadata.command_name == "get"
      assert start_metadata.command_length == 2

      # Verify stop event was emitted
      assert_receive {:telemetry_event, [:redis_cluster, :command, :stop], stop_measurements,
                      stop_metadata}

      assert Map.has_key?(stop_measurements, :duration)
      assert stop_measurements.duration > 0
      assert stop_metadata.config_name == :test_config
      assert stop_metadata.result == "test_value"

      # Cleanup
      :telemetry.detach(ref)
    end

    test "execute_command/3 emits exception event on error" do
      # Setup telemetry handler
      ref = make_ref()

      :telemetry.attach(
        ref,
        [:redis_cluster, :command, :exception],
        &TestHandler.handle_event/4,
        self()
      )

      # Execute a command that raises an exception
      command = ["SET", "test_key", "value"]

      metadata = %{
        config_name: :test_config,
        key: "test_key",
        role: :master,
        slot: 1234
      }

      assert_raise RuntimeError, "test error", fn ->
        Telemetry.execute_command(command, metadata, fn ->
          raise "test error"
        end)
      end

      # Verify exception event was emitted
      assert_receive {:telemetry_event, [:redis_cluster, :command, :exception], measurements,
                      exception_metadata}

      assert Map.has_key?(measurements, :duration)
      assert exception_metadata.config_name == :test_config
      assert exception_metadata.kind == :error
      assert %RuntimeError{} = exception_metadata.reason

      # Cleanup
      :telemetry.detach(ref)
    end

    test "execute_pipeline/3 emits events with pipeline metadata" do
      # Setup telemetry handler
      ref = make_ref()

      :telemetry.attach(
        ref,
        [:redis_cluster, :pipeline, :stop],
        &TestHandler.handle_event/4,
        self()
      )

      # Execute a pipeline with telemetry
      commands = [["GET", "key1"], ["SET", "key2", "value"], ["DEL", "key3"]]

      metadata = %{
        config_name: :test_config,
        key: "key1",
        role: :master,
        slot: 1234
      }

      result =
        Telemetry.execute_pipeline(commands, metadata, fn ->
          ["value1", "OK", 1]
        end)

      # Verify the result
      assert result == ["value1", "OK", 1]

      # Verify pipeline event was emitted
      assert_receive {:telemetry_event, [:redis_cluster, :pipeline, :stop], measurements,
                      pipeline_metadata}

      assert Map.has_key?(measurements, :duration)
      assert pipeline_metadata.config_name == :test_config
      assert pipeline_metadata.pipeline_size == 3
      assert pipeline_metadata.command_names == ["get", "set", "del"]

      # Cleanup
      :telemetry.detach(ref)
    end

    test "cluster_rediscovery/1 emits rediscovery event" do
      # Setup telemetry handler
      ref = make_ref()

      :telemetry.attach(
        ref,
        [:redis_cluster, :cluster, :rediscovery],
        &TestHandler.handle_event/4,
        self()
      )

      # Emit rediscovery event
      Telemetry.cluster_rediscovery(%{config_name: :test_config})

      # Verify rediscovery event was emitted
      assert_receive {:telemetry_event, [:redis_cluster, :cluster, :rediscovery], measurements,
                      metadata}

      assert measurements.count == 1
      assert metadata.config_name == :test_config
      assert Map.has_key?(metadata, :timestamp)

      # Cleanup
      :telemetry.detach(ref)
    end

    test "connection_acquired/1 emits connection event" do
      # Setup telemetry handler
      ref = make_ref()

      :telemetry.attach(
        ref,
        [:redis_cluster, :connection, :acquired],
        &TestHandler.handle_event/4,
        self()
      )

      # Emit connection acquired event
      Telemetry.connection_acquired(%{
        config_name: :test_config,
        host: "localhost",
        port: 6379,
        index: 0,
        pid: self()
      })

      # Verify connection event was emitted
      assert_receive {:telemetry_event, [:redis_cluster, :connection, :acquired], measurements,
                      metadata}

      assert measurements.count == 1
      assert metadata.config_name == :test_config
      assert metadata.host == "localhost"
      assert metadata.port == 6379

      # Cleanup
      :telemetry.detach(ref)
    end
  end
end
