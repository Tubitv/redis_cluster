defmodule RedisCluster.Monitor do
  @moduledoc """
  A Redis monitoring client for debugging and testing purposes.

  This module provides functionality to monitor Redis commands being executed
  on Redis cluster nodes. It collects commands internally and allows querying
  them on demand.

  **Warning**: MONITOR has a significant performance impact on Redis servers.
  Use only for debugging and testing, never in production under load.

  ## Example Usage

      # Start monitoring a specific node
      {:ok, monitor} = RedisCluster.Monitor.start_link(
        host: "localhost",
        port: 6379,
        max_commands: 100  # Keep only the 100 most recent commands
      )

      # Let some commands execute...
      Process.sleep(5000)

      # Get the collected commands
      commands = RedisCluster.Monitor.get_commands(monitor)

      for command <- commands do
        IO.puts("Command: " <> command.message)
      end

      # Clear the command history
      RedisCluster.Monitor.clear_commands(monitor)

      # Stop monitoring
      RedisCluster.Monitor.stop(monitor)
  """

  use GenServer
  require Logger

  alias RedisCluster.Configuration
  alias RedisCluster.Monitor.Message

  defstruct [:host, :port, :conn, :commands, :max_commands]

  @typedoc "A reference returned when starting monitoring"
  @type monitor_ref() :: reference()

  @doc """
  Starts a monitor process.

  ## Options

    * `:host` - Redis host (required)
    * `:port` - Redis port (required)
    * `:max_commands` - Maximum number of commands to keep in memory (optional, unlimited if nil, default 100)
  """
  @spec start_link(Keyword.t()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc """
  Gets the collected monitoring commands from the monitor.

  Returns a list of command maps, with the most recent commands first.
  Each command map contains:
    - `:host` - The Redis host
    - `:port` - The Redis port
    - `:message` - The raw monitoring message from Redis
    - `:timestamp` - The timestamp when the command was captured (milliseconds)
  """
  @spec get_commands(GenServer.server()) :: [map()]
  def get_commands(monitor) do
    GenServer.call(monitor, :get_commands)
  end

  @doc """
  Clears all collected commands from the monitor.
  """
  @spec clear_commands(GenServer.server()) :: :ok
  def clear_commands(monitor) do
    GenServer.call(monitor, :clear_commands)
  end

  @doc """
  Stops the monitor process.
  """
  @spec stop(GenServer.server()) :: :ok
  def stop(monitor) do
    GenServer.stop(monitor)
  end

  @doc """
  Starts monitoring on a single node in the cluster.

  Returns `{:ok, monitor_pid}` where monitor_pid can be used to query commands
  with `get_commands/1`.

  ## Options

    * `:host` - Redis host (required)
    * `:port` - Redis port (required)
    * `:max_commands` - Maximum number of commands to keep in memory (optional, unlimited if nil, default 100)
  """
  @spec monitor_node(String.t(), non_neg_integer(), Keyword.t()) ::
          {:ok, pid()} | {:error, term()}
  def monitor_node(host, port, opts \\ []) do
    max_commands = Keyword.get(opts, :max_commands, 100)

    monitor_opts = [
      host: host,
      port: port,
      max_commands: max_commands
    ]

    start_link(monitor_opts)
  end

  @doc """
  Starts monitoring on multiple cluster nodes based on role.

  Returns `{:ok, monitors}` where monitors is a list of tuples:
  `{host, port, role, monitor_pid}`.
  """
  @spec monitor_cluster_nodes(Configuration.t(), Keyword.t()) ::
          {:ok, [{String.t(), non_neg_integer(), atom(), pid()}]} | {:error, term()}
  def monitor_cluster_nodes(config, opts \\ []) do
    role_selector = Keyword.get(opts, :role, :any)

    nodes =
      for {_mod, _lo, _hi, role, host, port} <- RedisCluster.HashSlots.all_slots(config),
          role_selector == :any or role == role_selector,
          uniq: true do
        {host, port, role}
      end

    results =
      Enum.map(nodes, fn {host, port, role} ->
        case monitor_node(host, port, opts) do
          {:ok, monitor_pid} ->
            {:ok, {host, port, role, monitor_pid}}

          error ->
            Logger.warning("Failed to start monitoring for #{host}:#{port}", error: error)
            {:error, {host, port, error}}
        end
      end)

    # Check if all succeeded
    case Enum.split_with(results, &match?({:ok, _}, &1)) do
      {successes, []} ->
        monitors = Enum.map(successes, fn {:ok, result} -> result end)
        {:ok, monitors}

      {_successes, errors} ->
        {:error, errors}
    end
  end

  ## GenServer Implementation

  @impl GenServer
  def init(opts) do
    host = Keyword.fetch!(opts, :host)
    port = Keyword.fetch!(opts, :port)
    max_commands = Keyword.get(opts, :max_commands)

    state = %__MODULE__{
      host: host,
      port: port,
      conn: nil,
      commands: [],
      max_commands: max_commands
    }

    {:ok, state, {:continue, :connect}}
  end

  @impl GenServer
  def handle_continue(:connect, state) do
    case connect(state) do
      {:ok, new_state} ->
        {:noreply, new_state}

      {:error, reason} ->
        Logger.error(
          "Failed to connect to Redis (#{state.host}:#{state.port}) for monitoring #{inspect(reason)}",
          host: state.host,
          port: state.port,
          reason: reason
        )

        # Retry connection after a delay
        Process.send_after(self(), :retry_connect, 1000)
        {:noreply, %{state | conn: nil}}
    end
  end

  @impl GenServer
  def handle_call(:get_commands, _from, state) do
    {:reply, state.commands, state}
  end

  def handle_call(:clear_commands, _from, state) do
    {:reply, :ok, %{state | commands: []}}
  end

  @impl GenServer
  def handle_info({:tcp, conn, data}, %{conn: conn} = state) do
    clean_data = String.trim(data)

    case Message.parse(clean_data, state.host, state.port) do
      message = %Message{} ->
        new_commands = [message | state.commands]

        # Limit to max_commands if specified
        limited_commands =
          case state.max_commands do
            nil ->
              new_commands

            max when length(new_commands) > max ->
              Enum.take(new_commands, max)

            _max ->
              new_commands
          end

        {:noreply, %{state | commands: limited_commands}}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:tcp_closed, conn}, %{conn: conn} = state) do
    Logger.warning("Redis monitoring connection closed",
      host: state.host,
      port: state.port
    )

    # Clear connection and attempt to reconnect
    new_state = %{state | conn: nil}
    Process.send_after(self(), :retry_connect, 1000)
    {:noreply, new_state}
  end

  def handle_info({:tcp_error, conn, reason}, %{conn: conn} = state) do
    Logger.warning("Redis monitoring connection error",
      host: state.host,
      port: state.port,
      reason: reason
    )

    # Clear connection and attempt to reconnect
    new_state = %{state | conn: nil}
    Process.send_after(self(), :retry_connect, 1000)
    {:noreply, new_state}
  end

  def handle_info(:retry_connect, state) do
    case connect(state) do
      {:ok, new_state} ->
        {:noreply, new_state}

      {:error, reason} ->
        Logger.error(
          "Failed to reconnect to Redis (#{state.host}:#{state.port}) for monitoring #{inspect(reason)}",
          host: state.host,
          port: state.port,
          reason: reason
        )

        Process.send_after(self(), :retry_connect, 5000)
        {:noreply, state}
    end
  end

  def handle_info(_message, state) do
    {:noreply, state}
  end

  @impl GenServer
  def terminate(_reason, state) do
    if state.conn do
      _ = :gen_tcp.send(state.conn, "*1\r\n$4\r\nQUIT\r\n")
      :gen_tcp.close(state.conn)
    end

    :ok
  end

  ## Private Functions

  defp connect(state) do
    if state.conn do
      {:error, :already_connected}
    else
      # Use raw TCP connection for MONITOR
      case :gen_tcp.connect(String.to_charlist(state.host), state.port, [:binary, active: true]) do
        {:ok, socket} ->
          # Send MONITOR command using Redis protocol
          _ = :gen_tcp.send(socket, "*1\r\n$7\r\nMONITOR\r\n")
          {:ok, %{state | conn: socket}}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end
end
