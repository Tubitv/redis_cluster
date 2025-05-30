defmodule RedisCluster.Telemetry do
  @moduledoc """
  Telemetry integration for Redis cluster operations.

  This module provides functions to emit telemetry events for various Redis
  cluster operations including:

  - Redis command execution (get, set, delete, etc.)
  - Pipeline operations
  - Cluster discovery and resharding
  - Connection management
  - Error tracking

  ## Events

  The following telemetry events are emitted:

  ### Command Events
  - `[:redis_cluster, :command, :start]` - Before executing a Redis command
  - `[:redis_cluster, :command, :stop]` - After successful command execution
  - `[:redis_cluster, :command, :exception]` - When a command raises an exception

  ### Pipeline Events
  - `[:redis_cluster, :pipeline, :start]` - Before executing a pipeline
  - `[:redis_cluster, :pipeline, :stop]` - After successful pipeline execution
  - `[:redis_cluster, :pipeline, :exception]` - When a pipeline raises an exception

  ### Cluster Events
  - `[:redis_cluster, :cluster, :discovery, :start]` - Before cluster discovery
  - `[:redis_cluster, :cluster, :discovery, :stop]` - After cluster discovery
  - `[:redis_cluster, :cluster, :rediscovery]` - When cluster is rediscovered due to MOVED errors

  ### Connection Events
  - `[:redis_cluster, :connection, :acquired]` - When a connection is acquired from pool

  ## Example Usage

      # Basic telemetry handler
      :telemetry.attach(
        "redis-cluster-handler",
        [:redis_cluster, :command, :stop],
        &MyApp.TelemetryHandler.handle_command/4,
        nil
      )

      # Using telemetry_metrics
      Telemetry.Metrics.counter("redis_cluster.command.stop.count", tags: [:command_name])
      Telemetry.Metrics.distribution("redis_cluster.command.stop.duration",
        unit: {:native, :millisecond}
      )
  """

  @doc """
  Executes a telemetry event for a Redis command with timing.
  """
  @spec span(
          event :: [atom()],
          metadata :: map(),
          fun :: (() -> result)
        ) :: result
        when result: any()
  def span(event, metadata, fun) do
    start_time = System.monotonic_time()
    start_metadata = Map.put(metadata, :system_time, System.system_time())

    :telemetry.execute(
      event ++ [:start],
      %{system_time: System.system_time()},
      start_metadata
    )

    try do
      result = fun.()

      duration = System.monotonic_time() - start_time
      stop_metadata = Map.put(metadata, :result, result)

      :telemetry.execute(
        event ++ [:stop],
        %{duration: duration},
        stop_metadata
      )

      result
    rescue
      exception ->
        duration = System.monotonic_time() - start_time
        exception_metadata =
          metadata
          |> Map.put(:kind, :error)
          |> Map.put(:reason, exception)
          |> Map.put(:stacktrace, __STACKTRACE__)

        :telemetry.execute(
          event ++ [:exception],
          %{duration: duration},
          exception_metadata
        )

        reraise exception, __STACKTRACE__
    catch
      kind, reason ->
        duration = System.monotonic_time() - start_time
        exception_metadata =
          metadata
          |> Map.put(:kind, kind)
          |> Map.put(:reason, reason)
          |> Map.put(:stacktrace, __STACKTRACE__)

        :telemetry.execute(
          event ++ [:exception],
          %{duration: duration},
          exception_metadata
        )

        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  @doc """
  Emits a telemetry event for Redis command execution.
  """
  @spec execute_command(
          command :: [String.t()],
          metadata :: map(),
          fun :: (() -> result)
        ) :: result
        when result: any()
  def execute_command(command, metadata, fun) do
    command_name = command |> List.first() |> String.downcase()

    enhanced_metadata =
      metadata
      |> Map.put(:command, command)
      |> Map.put(:command_name, command_name)
      |> Map.put(:command_length, length(command))

    span([:redis_cluster, :command], enhanced_metadata, fun)
  end

  @doc """
  Emits a telemetry event for Redis pipeline execution.
  """
  @spec execute_pipeline(
          commands :: [[String.t()]],
          metadata :: map(),
          fun :: (() -> result)
        ) :: result
        when result: any()
  def execute_pipeline(commands, metadata, fun) do
    command_names = commands |> Enum.map(&(&1 |> List.first() |> String.downcase()))

    enhanced_metadata =
      metadata
      |> Map.put(:commands, commands)
      |> Map.put(:command_names, command_names)
      |> Map.put(:pipeline_size, length(commands))

    span([:redis_cluster, :pipeline], enhanced_metadata, fun)
  end

  @doc """
  Emits a telemetry event for cluster discovery operations.
  """
  @spec execute_discovery(
          metadata :: map(),
          fun :: (() -> result)
        ) :: result
        when result: any()
  def execute_discovery(metadata, fun) do
    span([:redis_cluster, :cluster, :discovery], metadata, fun)
  end

  @doc """
  Emits a simple telemetry event without timing.
  """
  @spec execute(event :: [atom()], measurements :: map(), metadata :: map()) :: :ok
  def execute(event, measurements \\ %{}, metadata \\ %{}) do
    :telemetry.execute(event, measurements, metadata)
  end

  @doc """
  Emits a telemetry event when the cluster needs to be rediscovered due to MOVED errors.
  """
  @spec cluster_rediscovery(metadata :: map()) :: :ok
  def cluster_rediscovery(metadata \\ %{}) do
    execute(
      [:redis_cluster, :cluster, :rediscovery],
      %{count: 1},
      Map.put(metadata, :timestamp, System.system_time())
    )
  end

  @doc """
  Emits a telemetry event when a connection is acquired from the pool.
  """
  @spec connection_acquired(metadata :: map()) :: :ok
  def connection_acquired(metadata \\ %{}) do
    execute(
      [:redis_cluster, :connection, :acquired],
      %{count: 1},
      metadata
    )
  end

  @doc """
  Emits telemetry events for cluster statistics.
  """
  @spec cluster_stats(stats :: map(), metadata :: map()) :: :ok
  def cluster_stats(stats, metadata \\ %{}) do
    execute(
      [:redis_cluster, :cluster, :stats],
      stats,
      Map.put(metadata, :timestamp, System.system_time())
    )
  end
end
