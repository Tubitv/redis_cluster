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

  defstruct [:host, :port, :conn, :filter, :command_count, :total_count, :commands, :max_commands]

  @typedoc """
  A function that filters commands based on the message struct.
  Also receives the current count of commands and the total count of commands.
  This can be used for advanced filtering, such reservoir sampling.
  If the function returns `:quit`, the monitor will stop.
  If the function returns `:drop`, the message will be dropped.
  If the function returns `:keep`, the message will be added to the queue, though an older message may be dropped.
  """
  @type filter_fun() :: (Message.t(),
                         current_count :: non_neg_integer(),
                         total_count :: non_neg_integer() ->
                           :keep | :drop | :quit)

  @typedoc "A reference returned when starting monitoring"
  @type monitor_ref() :: reference()

  @doc """
  Starts a monitor process.

  ## Options

    * `:host` - Redis host (required)
    * `:port` - Redis port (required)
    * `:max_commands` - Maximum number of commands to keep in memory (optional, unlimited if nil, default 100)
    * `:filter` - A function that filters commands based on the message struct (optional)
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
  Also resets the command count and total count.
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
  Returns a filter function that implements [reservoir sampling](https://samwho.dev/reservoir-sampling/).

  The gist of reservoir sampling is that it has an equal probability of keeping each item.
  This is useful for sampling a large stream of items without knowing the total number of items in advance.

  ## Parameters

  * `keep_count` - The number of items to keep.
  * `total_limit` - The total number of items to check before quitting.
  """
  @spec reservoir_sample_fun(keep_count :: non_neg_integer(), total_limit :: non_neg_integer()) ::
          filter_fun()
  def reservoir_sample_fun(keep_count, total_limit) do
    fn _message, _current_count, total_count ->
      cond do
        total_count >= total_limit ->
          :quit

        :rand.uniform() < keep_count / total_count ->
          :keep

        true ->
          :drop
      end
    end
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

    monitor_opts =
      Keyword.merge(opts,
        host: host,
        port: port,
        max_commands: max_commands
      )

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
    filter = Keyword.get(opts, :filter)

    if filter != nil and not is_function(filter, 3) do
      raise ArgumentError,
            "filter must be a function that takes 3 arguments: message, current_count, total_count"
    end

    state = %__MODULE__{
      host: host,
      port: port,
      conn: nil,
      filter: filter,
      command_count: 0,
      total_count: 0,
      commands: :queue.new(),
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
    # Convert queue to list with most recent commands first
    commands_list = :queue.to_list(state.commands)
    {:reply, commands_list, state}
  end

  def handle_call(:clear_commands, _from, state) do
    {:reply, :ok, %{state | commands: :queue.new(), command_count: 0, total_count: 0}}
  end

  @impl GenServer
  def handle_info({:tcp, conn, data}, %{conn: conn} = state) do
    # Split data by newlines and process each line separately
    # Multiple monitor messages can arrive in a single TCP packet
    lines =
      data
      |> String.split("\n")
      |> Enum.map(&String.trim/1)

    # Parse each line and collect valid messages
    new_messages =
      for l <- lines,
          l != "",
          msg = Message.parse(l, state.host, state.port),
          msg != nil do
        msg
      end

    case add_messages(
           state.commands,
           state.filter,
           state.command_count,
           state.total_count,
           state.max_commands,
           new_messages
         ) do
      {:ok, new_queue, new_command_count, new_total_count} ->
        # Update the state, then continue.
        {:noreply,
         %{
           state
           | commands: new_queue,
             command_count: new_command_count,
             total_count: new_total_count
         }}

      {:quit, new_queue, new_command_count, new_total_count} ->
        # Update the state, then quit the connection.
        new_state =
          %{
            state
            | commands: new_queue,
              command_count: new_command_count,
              total_count: new_total_count
          }

        {:noreply, quit(new_state)}
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
    quit(state)
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

  @spec add_messages(
          queue :: :queue.queue(),
          filter :: filter_fun() | nil,
          command_count :: non_neg_integer(),
          total_count :: non_neg_integer(),
          limit :: non_neg_integer(),
          messages :: [Message.t()]
        ) ::
          {:ok, :queue.queue(), command_count :: non_neg_integer(),
           total_count :: non_neg_integer()}
          | {:quit, :queue.queue(), command_count :: non_neg_integer(),
             total_count :: non_neg_integer()}
  defp add_messages(queue, _filter, command_count, total_count, _limit, []) do
    {:ok, queue, command_count, total_count}
  end

  defp add_messages(queue, filter, command_count, total_count, limit, [message | rest]) do
    action = message_action(message, filter, command_count, total_count)

    cond do
      action == :quit ->
        {:quit, queue, command_count, total_count}

      action == :keep and command_count == limit ->
        # Add the message and drop the oldest one.
        # Also, increment the total count.
        new_queue = :queue.in(message, queue)
        {_, new_queue} = :queue.out(new_queue)
        add_messages(new_queue, filter, command_count, total_count + 1, limit, rest)

      action == :keep ->
        # Add the message and increment the counts
        new_queue = :queue.in(message, queue)
        add_messages(new_queue, filter, command_count + 1, total_count + 1, limit, rest)

      action == :drop ->
        add_messages(queue, filter, command_count, total_count + 1, limit, rest)
    end
  end

  defp message_action(_message, nil, _current_count, _total_count) do
    :keep
  end

  defp message_action(message, filter, current_count, total_count) do
    # Add 1 for the current message.
    # This also avoids division by zero.
    filter.(message, current_count, total_count + 1)
  end

  defp quit(state) do
    if state.conn do
      _ = :gen_tcp.send(state.conn, "*1\r\n$4\r\nQUIT\r\n")
      :gen_tcp.close(state.conn)
    end

    %{state | conn: nil}
  end
end
