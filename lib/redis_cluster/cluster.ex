defmodule RedisCluster.Cluster do
  @moduledoc """
  The main module for interacting with a Redis cluster. Typically you will use the
  `RedisCluster` module. Though this module can be used directly for more dynamic use
  cases, such as connecting to a Redis cluster at runtime or in Livebook demos.
  """

  alias RedisCluster.Key
  alias RedisCluster.HashSlots
  alias RedisCluster.Configuration
  alias RedisCluster.Telemetry

  use Supervisor

  require Logger

  @typedoc "A key in Redis, which can be a binary, atom, or number. It will be converted to a string."
  @type key() :: binary() | atom() | number()

  @typedoc "A value in Redis, which can be a binary, atom, or number."
  @type value() :: binary() | atom() | number()

  @typedoc "A list of key-value pairs or a map of key-value pairs."
  @type pairs() :: [{key(), value()}] | %{key() => value()}

  @typedoc "A Redis command, which is a list of binary strings."
  @type command() :: [binary()]

  @typedoc "A list of commands. Effectively a list of lists."
  @type pipeline() :: [command()]

  @typedoc "The role of a Redis node. Either a master or a replica."
  @type role() :: :master | :replica

  @doc false
  def start_link(config = %Configuration{}) do
    Supervisor.start_link(__MODULE__, config, name: config.cluster)
  end

  @doc false
  def child_spec(config = %Configuration{}, _opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [config]},
      type: :supervisor,
      restart: :permanent,
      shutdown: 5000
    }
  end

  @impl Supervisor
  def init(config = %Configuration{}) do
    children = [
      {Registry, keys: :unique, name: config.registry},
      {RedisCluster.Pool, config},
      {RedisCluster.ShardDiscovery, config}
    ]

    # This could probably be :rest_for_one

    Supervisor.init(children, strategy: :one_for_all)
  end

  @spec get(RedisCluster.Configuration.t(), atom() | binary() | number()) ::
          nil | binary() | {:error, any()}
  @doc """
  Calls the [Redis `GET` command](https://redis.io/commands/get).

  Options:
    * `:compute_hash_tag` - Whether to compute the hash tag of the key (default `false`).
    * `:role` - The role to use when querying the cluster. Possible values are:
      - `:master` - Query the master node (default).
      - `:replica` - Query a replica node.
      - `:any` - Query any node.
  """
  @spec get(Configuration.t(), key(), Keyword.t()) :: binary() | nil | {:error, any()}
  def get(config, key, opts \\ []) do
    key = to_string(key)
    role = Keyword.get(opts, :role, :master)
    slot = Key.hash_slot(key, opts)

    metadata = %{
      config_name: config.name,
      key: key,
      role: role,
      slot: slot
    }

    Telemetry.execute_command(["GET", key], metadata, fn ->
      command_with_retry(config, role, key, ["GET", key], opts)
    end)
  end

  @doc """
  Calls the [Redis `SET` command](https://redis.io/commands/set).

  By default doesn't set an expiration time for the key. Only `:expire_seconds` or
  `:expire_milliseconds` can be set, not both.

  Since this is a write command, it will always target master nodes.

  You may instruct the server to not wait for replies by setting `:reply` to `false`.
  This may reduce latency and reduce processing on the server.
  Be aware that you won't know if the command was successful or not.
  If you are using `:only_new` or `:only_overwrite`, you also won't know if the key was set.
  Furthermore, if you follow up with `GET` commands for the same key, the `SET` command
  may not have been processed yet.

  Options:
    * `:compute_hash_tag` - Whether to compute the hash tag of the key (default `false`).
    * `:expire_seconds` - The number of seconds until the key expires.
    * `:expire_milliseconds` - The number of milliseconds until the key expires.
    * `:reply` - Whether to wait for replies from the server. (default `true`)
    * `:set` - Controls when the key should be set. Possible values are:
      - `:always` - Always set the key (default).
      - `:only_overwrite` - Only set the key if it already exists.
      - `:only_new` - Only set the key if it doesn't exist.
  """
  @spec set(Configuration.t(), key(), value(), Keyword.t()) :: :ok | {:error, any()}
  def set(config, key, value, opts \\ []) do
    key = to_string(key)
    role = :master
    slot = Key.hash_slot(key, opts)

    command =
      List.flatten([
        ["SET", key, value],
        expire_option(opts),
        write_option(opts)
      ])

    metadata = %{
      config_name: config.name,
      key: key,
      value: value,
      role: role,
      slot: slot,
      opts: opts
    }

    Telemetry.execute_command(command, metadata, fn ->
      case command_with_retry(config, role, key, command, opts) do
        error = {:error, _} -> error
        "OK" -> :ok
        other -> other
      end
    end)
  end

  @doc """
  Calls the [Redis `DEL` command](https://redis.io/docs/latest/commands/del).

  Returns 1 if the key was deleted, 0 if the key was not found.

  Since this is a write command, it will always target master nodes.

  Options:
    * `:compute_hash_tag` - Whether to compute the hash tag of the key (default `false`).
  """
  @spec delete(Configuration.t(), key(), Keyword.t()) :: integer() | {:error, any()}
  def delete(config, key, opts \\ []) do
    key = to_string(key)
    role = :master
    slot = Key.hash_slot(key, opts)

    metadata = %{
      config_name: config.name,
      key: key,
      role: role,
      slot: slot
    }

    Telemetry.execute_command(["DEL", key], metadata, fn ->
      command_with_retry(config, role, key, ["DEL", key], opts)
    end)
  end

  @doc """
  Calls the [Redis `DEL` command](https://redis.io/docs/latest/commands/del) without waiting for a reply.

  This function does not wait for the server to confirm the deletion, which may reduce
  latency and reduce processing on the server. However, you won't know if the command
  was successful or not. Furthermore, if you follow up with `GET` commands for the same key,
  the `DEL` command may not have been processed yet.

  Since this is a write command, it will always target master nodes.

  Unlike `delete/3`, this function does not return the number of keys deleted.
  It will always return `:ok` or an error.

  Options:
    * `:compute_hash_tag` - Whether to compute the hash tag of the key (default `false`).
  """
  @spec delete_noreply(Configuration.t(), key(), Keyword.t()) :: :ok | {:error, any()}
  def delete_noreply(config, key, opts \\ []) do
    key = to_string(key)
    role = :master
    slot = Key.hash_slot(key, opts)

    metadata = %{
      config_name: config.name,
      key: key,
      role: role,
      slot: slot
    }

    Telemetry.execute_command(["DEL", key], metadata, fn ->
      opts = Keyword.put(opts, :reply, false)
      command_with_retry(config, role, key, ["DEL", key], opts)
    end)
  end

  @doc """
  **WARNING**: This command is not a one-to-one mapping to the
  [Redis `MSET` command](https://redis.io/docs/latest/commands/mset/).

  All the keys in the list must hash to the same slot.
  This means they must hash to the exact value.
  It doesn't matter if those values map to the same node.
  If any key doesn't hash to the same value, the `MSET` command will fail.

  The only way to guarantee keys hash to the same slot is to use a
  [hash tag](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/#hash-tags).
  Because of this, `:compute_hash_tag` is set to `true` by default.

  Rather than use `MSET` this command sends a `SET` command for each key-value pair.
  Where possible, the `SET` commands are sent in a pipeline, saving one or more round trips.

  Since `SET` is used, this function offers expiration and write options like `set/3`.

  The downside to this approach is if the cluster reshards.
  The function will attempt all the `SET` commands, even if some of them will fail.
  Then it tries to rediscover the cluster if any of the commands failed.
  Though you will need to try your command again to ensure all the keys are set.

  Since this sends write commands, it will always be target master nodes.

  This function cannot guarantee which value is set when a key is included
  multiple times in one call.

  The key-value pairs can be given as a list of tuples or a map.

  Commands are sent sequentially for simplicity.
  This means the function will be slower than sending commands in parallel.
  If you need to set many keys in parallel, consider using `set_many_async/3` instead.

  You may instruct the server to not return replies by setting `:reply` to `false`.
  This can save on bandwidth, reduce latency, and reduce processing on the server.
  Be aware that you won't know if the commands were successful or not. If you are using
  `:only_new` or `:only_overwrite`, you also won't know which keys were set.
  Furthermore, if you follow up with `GET` commands for the same keys, the `SET` commands
  may not have been processed yet.

  Options:
    * `:compute_hash_tag` - Whether to compute the hash tag of the key (default `true`).
    * `:expire_seconds` - The number of seconds until the key expires.
    * `:expire_milliseconds` - The number of milliseconds until the key expires.
    * `:reply` - Whether to return replies from the server. (default `true`)
    * `:set` - Controls when the key should be set. Possible values are:
      - `:always` - Always set the key (default).
      - `:only_overwrite` - Only set the key if it already exists.
      - `:only_new` - Only set the key if it doesn't exist.
  """
  @spec set_many(Configuration.t(), pairs(), Keyword.t()) :: :ok | [{:error, any()}]
  def set_many(config, pairs, opts \\ [])

  def set_many(_config, pairs, _opts)
      when pairs == []
      when is_map(pairs) and map_size(pairs) == 0 do
    :ok
  end

  def set_many(config, [{k, v}], opts) do
    opts = Keyword.merge([compute_hash_tag: true], opts)
    set(config, k, v, opts)
  end

  def set_many(config, map, opts) when is_map(map) and map_size(map) == 1 do
    [{k, v}] = Map.to_list(map)
    opts = Keyword.merge([compute_hash_tag: true], opts)
    set(config, k, v, opts)
  end

  def set_many(config, pairs, opts) do
    opts = Keyword.merge([compute_hash_tag: true], opts)
    reply? = Keyword.get(opts, :reply, true)
    commands_by_conn = create_set_cmds_by_conn(pairs, config, opts)
    redis = config.redis_module

    # Run the commands and collect the errors in a single loop.
    errors =
      if reply? do
        for {conn, cmds} <- commands_by_conn,
            response = redis.pipeline(conn, cmds),
            not match?({:ok, _}, response) do
          response
        end
      else
        for {conn, cmds} <- commands_by_conn,
            response = redis.noreply_pipeline(conn, cmds),
            response != :ok do
          response
        end
      end

    case errors do
      [] -> :ok
      errors -> maybe_rediscover(config, errors)
    end
  end

  @doc """
  Similar to `set_many/3` but uses `Task.async_stream/3` to set the values in parallel.

  Options:
    * `:max_concurrency` - The maximum number of concurrent tasks to run (default `System.schedulers_online()`).
    * `:timeout` - The max time in milliseconds to wait for each task to complete (default `5000`).
    * `:compute_hash_tag` - Whether to compute the hash tag of the key (default `true`).
    * `:reply` - Whether to return replies from the server. (default `true`)
  """
  @spec set_many_async(Configuration.t(), pairs(), Keyword.t()) :: :ok | [{:error, any()}]
  def set_many_async(config, pairs, opts \\ [])

  def set_many_async(_config, pairs, _opts)
      when pairs == []
      when is_map(pairs) and map_size(pairs) == 0 do
    :ok
  end

  def set_many_async(config, [{k, v}], opts) do
    opts = Keyword.merge([compute_hash_tag: true], opts)
    set(config, k, v, opts)
  end

  def set_many_async(config, map, opts) when is_map(map) and map_size(map) == 1 do
    [{k, v}] = Map.to_list(map)
    opts = Keyword.merge([compute_hash_tag: true], opts)
    set(config, k, v, opts)
  end

  def set_many_async(config, pairs, opts) do
    opts = Keyword.merge([compute_hash_tag: true], opts)
    reply? = Keyword.get(opts, :reply, true)
    commands_by_conn = create_set_cmds_by_conn(pairs, config, opts)

    # Run the commands and collect the errors.
    errors =
      if reply? do
        commands_by_conn
        |> run_async_commands_by_conn(config, opts)
        |> Enum.reject(&match?({:ok, _}, &1))
      else
        commands_by_conn
        |> run_async_commands_by_conn(config, opts)
        |> Enum.reject(&(&1 == :ok))
      end

    case errors do
      [] -> :ok
      errors -> maybe_rediscover(config, errors)
    end
  end

  @doc """
  **WARNING**: This command is not a one-to-one mapping to the
  [Redis `MGET` command](https://redis.io/docs/latest/commands/mget/).
  See the `set_many/3` function for details why.

  Returns the values in the same order as the given keys.
  If a key is not found, `nil` is returned in its place.

  Options:
    * `:compute_hash_tag` - Whether to compute the hash tag of the key (default `true`).
    * `:role` - The role to use when querying the cluster. Possible values are:
      - `:master` - Query the master node (default).
      - `:replica` - Query a replica node.
      - `:any` - Query any node.
  """
  @spec get_many(Configuration.t(), [key()], Keyword.t()) :: [String.t() | nil]
  def get_many(config, keys, opts \\ [])

  def get_many(_config, [], _opts) do
    []
  end

  def get_many(config, [key], opts) do
    opts = Keyword.merge([compute_hash_tag: true], opts)
    [get(config, key, opts)]
  end

  def get_many(config, keys, opts) do
    opts = Keyword.merge([compute_hash_tag: true], opts)
    role = Keyword.get(opts, :role, :master)
    keys = Enum.map(keys, &to_string/1)

    values_by_key =
      keys
      |> MapSet.new(&to_string/1)
      |> Enum.group_by(&Key.hash_slot(&1, opts))
      |> Enum.flat_map(fn {_slot, key_batch} ->
        case command_with_retry(config, role, key_batch, ["MGET" | key_batch], opts) do
          {:error, _} -> []
          values when is_list(values) -> Enum.zip(key_batch, values)
        end
      end)

    # Ensures the values are returned in the same order they were requested.
    for key <- keys do
      :proplists.get_value(key, values_by_key, nil)
    end
  end

  @doc """
  Similar to `get_many/3` but uses `Task.async_stream/3` to fetch the values in parallel.

  Options:
    * `:max_concurrency` - The maximum number of concurrent tasks to run (default `System.schedulers_online()`).
    * `:timeout` - The max time in milliseconds to wait for each task to complete (default `5000`).
    * `:compute_hash_tag` - Whether to compute the hash tag of the key (default `true`).
    * `:role` - The role to use when querying the cluster. Possible values are:
      - `:master` - Query the master node (default).
      - `:replica` - Query a replica node.
      - `:any` - Query any node.
  """
  @spec get_many_async(Configuration.t(), [key()], Keyword.t()) :: [String.t() | nil]
  def get_many_async(config, keys, opts \\ [])

  def get_many_async(_config, [], _opts) do
    []
  end

  def get_many_async(config, keys, opts) do
    opts = Keyword.merge([compute_hash_tag: true], opts)
    role = Keyword.get(opts, :role, :master)
    keys = Enum.map(keys, &to_string/1)
    max_concurrency = Keyword.get(opts, :max_concurrency) || System.schedulers_online()
    timeout = Keyword.get(opts, :timeout, 5000)

    task_opts = [
      max_concurrency: max_concurrency,
      ordered: false,
      timeout: timeout,
      on_timeout: :kill_task,
      zip_input_on_exit: true
    ]

    values_by_key =
      keys
      |> MapSet.new(&to_string/1)
      |> Enum.group_by(&Key.hash_slot(&1, opts))
      |> Task.async_stream(
        fn {_slot, key_batch} ->
          case command_with_retry(config, role, key_batch, ["MGET" | key_batch], opts) do
            {:error, _} -> []
            values when is_list(values) -> Enum.zip(key_batch, values)
          end
        end,
        task_opts
      )
      |> Enum.flat_map(fn
        {:ok, values} ->
          values

        {:error, reason} ->
          Logger.warning("Failed to get values for keys", reason: reason)
          []
      end)

    # Ensures the values are returned in the same order they were requested.
    for key <- keys do
      :proplists.get_value(key, values_by_key, nil)
    end
  end

  @doc """
  **WARNING**: This command is not a one-to-one mapping to the
  [Redis `DEL` command](https://redis.io/docs/latest/commands/del/).
  See the `set_many/3` function for details why.

  Deletes the listed keys and returns the number of keys that were deleted.
  Only unique `DEL` commands are sent.
  If there are duplicate keys, this number deleted will be less than total keys given.

  Since this is a write command, it will always target master nodes.

  Stops deleting keys if a `MOVED` error is encountered.

  You may instruct the server to not return replies by setting `:reply` to `false`.
  This can save on bandwidth, reduce latency, and reduce processing on the server.
  If you follow up with `GET` commands for the same keys, the `DEL` commands may
  not have been processed yet. This means you may still get a value for the keys.

  Options:
    * `:compute_hash_tag` - Whether to compute the hash tag of the key (default `true`).
    * `:reply` - Whether to return replies from the server. (default `true`)
  """
  @spec delete_many(Configuration.t(), [key()], Keyword.t()) :: integer() | :ok | {:error, any()}
  def delete_many(config, keys, opts \\ [])

  def delete_many(_config, [], _opts) do
    0
  end

  def delete_many(config, [key], opts) do
    reply? = Keyword.get(opts, :reply, true)

    if reply? do
      delete(config, key, opts)
    else
      delete_noreply(config, key, opts)
    end
  end

  def delete_many(config, keys, opts) when is_list(keys) do
    opts = Keyword.merge([compute_hash_tag: true], opts)
    reply? = Keyword.get(opts, :reply, true)
    redis = config.redis_module

    commands_by_conn = create_del_cmds_by_conn(keys, config, opts)

    responses =
      if reply? do
        for {conn, cmds} <- commands_by_conn do
          redis.pipeline(conn, cmds)
        end
      else
        for {conn, cmds} <- commands_by_conn do
          redis.noreply_pipeline(conn, cmds)
        end
      end

    process_del_responses(responses, config, :master, keys, reply?)
  end

  @doc """
  Similar to `delete_many/3` but uses `Task.async_stream/3` to delete the keys in parallel.

  Options:
    * `:max_concurrency` - The maximum number of concurrent tasks to run (default `System.schedulers_online()`).
    * `:timeout` - The max time in milliseconds to wait for each task to complete (default `5000`).
    * `:compute_hash_tag` - Whether to compute the hash tag of the key (default `true`).
    * `:reply` - Whether to return replies from the server. (default `true`)
  """
  @spec delete_many_async(Configuration.t(), [key()], Keyword.t()) ::
          integer() | :ok | {:error, any()}
  def delete_many_async(config, keys, opts \\ [])

  def delete_many_async(_config, [], _opts) do
    0
  end

  def delete_many_async(config, [key], opts) do
    reply? = Keyword.get(opts, :reply, true)

    if reply? do
      delete(config, key, opts)
    else
      delete_noreply(config, key, opts)
    end
  end

  def delete_many_async(config, keys, opts) when is_list(keys) do
    opts = Keyword.merge([compute_hash_tag: true], opts)
    reply? = Keyword.get(opts, :reply, true)

    keys
    |> create_del_cmds_by_conn(config, opts)
    |> run_async_commands_by_conn(config, opts)
    |> process_del_responses(config, :master, keys, reply?)
  end

  @deprecated "Use `command/4` instead."
  @spec command(Configuration.t(), command(), Keyword.t()) :: term() | {:error, any()}
  def command(config, command, opts) do
    key = opts |> Keyword.fetch!(:key) |> to_string()
    command(config, command, key, opts)
  end

  @doc """
  Sends the given command to Redis.
  This allows sending any command to Redis that isn't already implemented in this module.

  Like `pipeline/4`, this function needs a key to determine which node to send the command to.
  If the command is safe to send to any node, you can use `:any` as the key.
  This will select a random node.

  Options:
    * `:compute_hash_tag` - Whether to compute the hash tag of the key (default `false`). See `RedisCluster.Key.hash_slot/2`.
    * `:role` - The role to use when querying the cluster. Possible values are:
      - `:master` - Query the master node (default).
      - `:replica` - Query a replica node.
      - `:any` - Query any node.
  """
  @spec command(Configuration.t(), command(), key() | :any, Keyword.t()) ::
          term() | {:error, any()}
  def command(config, command, key, opts) do
    role = Keyword.get(opts, :role, :master)

    metadata = %{
      config_name: config.name,
      key: key,
      role: role,
      slot: hash_slot(key, opts)
    }

    Telemetry.execute_command(command, metadata, fn ->
      command_with_retry(config, role, key, command, opts)
    end)
  end

  @deprecated "Use `pipeline/4` instead."
  @spec pipeline(Configuration.t(), pipeline(), Keyword.t()) :: [term()] | {:error, any()}
  def pipeline(config, commands, opts) do
    key = Keyword.fetch!(opts, :key)
    pipeline(config, commands, key, opts)
  end

  @doc """
  Sends a sequence of commands to Redis.
  This allows sending any command to Redis that isn't already implemented in this module.
  It sends the commands in one batch, reducing the number of round trips.

  Responses are returned as a list in the same order as the commands.

  The key is used to determine the hash slot which is mapped to a node.
  Commands like `GET` and `SET` only work with keys assigned to that node.
  Other commands like `DBSIZE` and `INFO MEMORY` can be sent to any node.
  If all commmands in the pipeline are safe to send to any node, you can use `:any` as the key.
  This will select a random node.

  Options:
    * `:compute_hash_tag` - Whether to compute the hash tag of the key (default `false`). See `RedisCluster.Key.hash_slot/2`.
    * `:role` - The role to use when querying the cluster. Possible values are:
      - `:master` - Query the master node (default).
      - `:replica` - Query a replica node.
      - `:any` - Query any node.
  """
  @spec pipeline(Configuration.t(), pipeline(), key() | :any, Keyword.t()) ::
          [term()] | {:error, any()}
  def pipeline(config, commands, key, opts) do
    role = Keyword.get(opts, :role, :master)

    metadata = %{
      config_name: config.name,
      key: key,
      slot: hash_slot(key, opts),
      role: role
    }

    Telemetry.execute_pipeline(commands, metadata, fn ->
      case pipeline_with_retry(config, role, key, commands, opts) do
        {:ok, results} -> results
        {:error, reason} -> {:error, reason}
      end
    end)
  end

  @doc """
  Sends the given pipeline to all nodes in the cluster.
  May filter on a specific role if desired, defaults to all nodes.

  Note that this sends a pipeline instead of a single command.
  You can issue as many commands as you like and get the raw results back.
  The pipelines are sent to each node sequentially, so this may take some time.
  If you want to send the commands in parallel, use `broadcast_async/3` instead.
  This is useful for debugging with commands like `DBSIZE` or `INFO MEMORY`.
  Be sure to only send commands that are safe to run on any node.

  Options:
    * `:role` - The role to use when querying the cluster. Possible values are:
      - `:any` - Query any node (default).
      - `:master` - Query the master nodes.
      - `:replica` - Query the replica nodes.
  """
  @spec broadcast(Configuration.t(), pipeline(), Keyword.t()) ::
          [
            {host :: String.t(), port :: non_neg_integer(), role :: role(),
             result :: {:ok, [Redix.Protocol.redis_value()]} | {:error, any()}}
          ]
  def broadcast(config, commands, opts \\ []) do
    role_selector = Keyword.get(opts, :role, :any)
    redis = config.redis_module

    for {_mod, _lo, _hi, role, host, port} <- HashSlots.all_slots(config),
        role_selector == :any or role == role_selector do
      conn = RedisCluster.Pool.get_conn(config, host, port)
      result = redis.pipeline(conn, commands)

      {host, port, role, result}
    end
  end

  @doc """
  Similar to `broadcast/3` but uses `Task.async_stream/3` to send the commands in parallel.

  The results are returned as a Stream. You can collect the results into a list.
  Or you can take the first N items. Be aware ordering is not guaranteed.

  Options:
    * `:max_concurrency` - The maximum number of concurrent tasks to run (default `System.schedulers_online()`).
    * `:timeout` - The max time in milliseconds to wait for each task to complete (default `5000`).
    * `:role` - The role to use when querying the cluster. Possible values are:
      - `:any` - Query any node (default).
      - `:master` - Query the master nodes.
      - `:replica` - Query the replica nodes.
  """
  @spec broadcast_async(Configuration.t(), pipeline(), Keyword.t()) ::
          Enumerable.t()
  def broadcast_async(config, commands, opts \\ []) do
    role_selector = Keyword.get(opts, :role, :any)
    max_concurrency = Keyword.get(opts, :max_concurrency) || System.schedulers_online()
    timeout = Keyword.get(opts, :timeout, 5000)
    redis = config.redis_module

    task_opts = [
      max_concurrency: max_concurrency,
      ordered: false,
      timeout: timeout,
      on_timeout: :kill_task,
      zip_input_on_exit: true
    ]

    command_info =
      for {_mod, _lo, _hi, role, host, port} <- HashSlots.all_slots(config),
          role_selector == :any or role == role_selector do
        conn = RedisCluster.Pool.get_conn(config, host, port)
        {host, port, role, conn, commands}
      end

    command_info
    |> Task.async_stream(
      fn {host, port, role, conn, cmds} ->
        result = redis.pipeline(conn, cmds)
        {host, port, role, result}
      end,
      task_opts
    )
    # Unwrap the nested {:ok, ...} tuples.
    |> Stream.map(fn
      {:ok, value} -> value
      other -> other
    end)
  end

  ## Helpers

  @spec run_async_commands_by_conn(
          commands_by_conn :: %{required(pid()) => pipeline()},
          Configuration.t(),
          task_opts :: Keyword.t()
        ) :: Enumerable.t()
  defp run_async_commands_by_conn(commands_by_conn, config, opts) do
    max_concurrency = Keyword.get(opts, :max_concurrency) || System.schedulers_online()
    timeout = Keyword.get(opts, :timeout, 5000)
    reply? = Keyword.get(opts, :reply, true)
    redis = config.redis_module

    task_opts = [
      max_concurrency: max_concurrency,
      ordered: false,
      timeout: timeout,
      on_timeout: :kill_task,
      zip_input_on_exit: true
    ]

    stream =
      if reply? do
        Task.async_stream(
          commands_by_conn,
          fn {conn, cmds} ->
            redis.pipeline(conn, cmds)
          end,
          task_opts
        )
      else
        Task.async_stream(
          commands_by_conn,
          fn {conn, cmds} ->
            redis.noreply_pipeline(conn, cmds)
          end,
          task_opts
        )
      end

    # Unwrap the nested {:ok, ...} tuples.
    Stream.map(stream, fn
      {:ok, value} -> value
      other -> other
    end)
  end

  @spec create_set_cmds_by_conn(
          pairs :: [{key(), value()}],
          Configuration.t(),
          opts :: Keyword.t()
        ) :: %{required(pid()) => pipeline()}
  defp create_set_cmds_by_conn(pairs, config, opts) do
    # Create a map of conn => [~w[SET key value ...], ...]
    pairs
    |> Map.new(fn {k, v} -> {to_string(k), v} end)
    |> Enum.group_by(
      # Find the conn for the key.
      fn {k, _v} ->
        slot = Key.hash_slot(k, opts)
        get_conn(config, slot, :master)
      end,
      # Create the SET command.
      fn {k, v} ->
        List.flatten([
          ["SET", k, v],
          expire_option(opts),
          write_option(opts)
        ])
      end
    )
  end

  @spec create_del_cmds_by_conn(
          keys :: [key()],
          Configuration.t(),
          opts :: Keyword.t()
        ) :: %{required(pid()) => pipeline()}
  defp create_del_cmds_by_conn(keys, config, opts) do
    # Create a map of conn => [~w[DEL key], ...]
    keys
    |> MapSet.new(&to_string/1)
    |> Enum.group_by(
      # Find the conn for the key.
      fn key ->
        slot = Key.hash_slot(key, opts)
        get_conn(config, slot, :master)
      end,
      # Create the DEL command.
      fn key -> ["DEL", key] end
    )
  end

  @spec process_del_responses(
          results :: Enumerable.t(),
          Configuration.t(),
          role(),
          [key()],
          reply? :: boolean()
        ) :: integer() | :ok | {:error, any()}
  defp process_del_responses(results, config, role, keys, _reply? = true) do
    Enum.reduce_while(results, 0, fn
      {:ok, results}, acc ->
        {:cont, acc + Enum.sum(results)}

      error = {:error, %Redix.Error{message: "MOVED" <> rest}}, _acc ->
        # If we get a MOVED error, we need to rediscover the cluster.
        {expected_slot, host, port} = parse_redirect(rest, config.host)

        metadata = %{
          role: role,
          expected_slot: expected_slot,
          expected_host: "#{host}:#{port}",
          keys: List.wrap(keys),
          slot: hash_slot(keys, []),
          config_name: config.name
        }

        Logger.warning("Received MOVED redirect with delete_many, rediscovering cluster.",
          metadata: metadata,
          table: HashSlots.all_slots_as_table(config)
        )

        Telemetry.moved_redirect(metadata)

        rediscover(config)
        {:halt, error}

      _error, acc ->
        {:cont, acc}
    end)
  end

  defp process_del_responses(list, _config, _role, _keys, _reply? = false) when is_list(list) do
    :ok
  end

  defp process_del_responses(stream, _config, _role, _keys, _reply? = false) do
    # Force the async stream to run.
    Stream.run(stream)
    :ok
  end

  defp expire_option(opts) do
    expire_seconds = Keyword.get(opts, :expire_seconds, 0)
    expire_milliseconds = Keyword.get(opts, :expire_milliseconds, 0)

    cond do
      expire_milliseconds > 0 -> ["PX", expire_milliseconds]
      expire_seconds > 0 -> ["EX", expire_seconds]
      true -> []
    end
  end

  defp write_option(opts) do
    case Keyword.get(opts, :set, :always) do
      :always -> []
      :only_overwrite -> ["XX"]
      :only_new -> ["NX"]
    end
  end

  @spec command_with_retry(
          Configuration.t(),
          role(),
          key_or_keys :: key() | [key()] | :any,
          command(),
          opts :: Keyword.t()
        ) :: Redix.Protocol.redis_value() | :ok | {:error, any()}
  defp command_with_retry(config, role, key_or_keys, command, opts) do
    conn = select_conn(config, key_or_keys, role, opts)

    case pipeline_with_retry(config, role, conn, key_or_keys, [command], opts) do
      :ok ->
        :ok

      {:ok, [error = %Redix.Error{}]} ->
        {:error, error}

      {:ok, [response]} ->
        response

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec pipeline_with_retry(
          Configuration.t(),
          role(),
          key_or_keys :: key() | [key()] | :any,
          commands :: pipeline(),
          opts :: Keyword.t()
        ) :: {:ok, [Redix.Protocol.redis_value()]} | {:error, any()}
  defp pipeline_with_retry(config, role, key_or_keys, commands, opts) do
    conn = select_conn(config, key_or_keys, role, opts)

    pipeline_with_retry(config, role, conn, key_or_keys, commands, opts)
  end

  @spec pipeline_with_retry(
          Configuration.t(),
          role(),
          conn :: pid(),
          key_or_keys :: key() | [key()] | :any,
          commands :: pipeline(),
          opts :: Keyword.t()
        ) :: :ok | {:ok, [Redix.Protocol.redis_value()]} | {:error, any()}
  defp pipeline_with_retry(config, role, conn, key_or_keys, commands, opts) do
    reply? = Keyword.get(opts, :reply, true)

    result =
      if reply? do
        config.redis_module.pipeline(conn, commands)
      else
        config.redis_module.noreply_pipeline(conn, commands)
      end

    case result do
      :ok ->
        :ok

      {:ok, result} ->
        {:ok, result}

      # The key wasn't on the expected node.
      # Try rediscovering the cluster.
      {:error, %Redix.Error{message: "MOVED " <> rest}} ->
        handle_moved_redirect(config, role, key_or_keys, commands, rest, opts)

      # A temporary redirect.
      {:error, %Redix.Error{message: "ASK " <> rest}} ->
        handle_ask_redirect(config, role, key_or_keys, commands, rest, opts)

      error = {:error, reason} ->
        Logger.warning("Failed to query Redis",
          role: role,
          keys: List.wrap(key_or_keys),
          slot: hash_slot(key_or_keys, opts),
          config_name: config.name,
          reason: reason
        )

        error
    end
  end

  defp handle_moved_redirect(config, role, key_or_keys, commands, rest, opts) do
    {expected_slot, host, port} = parse_redirect(rest, config.host)
    expected_host = "#{host}:#{port}"

    metadata = %{
      role: role,
      expected_slot: expected_slot,
      expected_host: expected_host,
      keys: List.wrap(key_or_keys),
      slot: hash_slot(key_or_keys, opts),
      config_name: config.name
    }

    Logger.warning("Received MOVED redirect, rediscovering cluster.",
      metadata: metadata,
      table: HashSlots.all_slots_as_table(config)
    )

    Telemetry.moved_redirect(metadata)

    rediscover(config)
    conn = get_conn(config, expected_slot, role)

    # Try one more time.
    config.redis_module.pipeline(conn, commands)
  end

  defp handle_ask_redirect(config, role, key_or_keys, commands, rest, opts) do
    {expected_slot, domain, port} = parse_redirect(rest, config.host)
    expected_host = "#{domain}:#{port}"

    metadata = %{
      role: role,
      expected_slot: expected_slot,
      expected_host: expected_host,
      keys: List.wrap(key_or_keys),
      slot: hash_slot(key_or_keys, opts),
      config_name: config.name
    }

    Logger.warning("Received ASK redirect",
      metadata: metadata,
      table: HashSlots.all_slots_as_table(config)
    )

    Telemetry.ask_redirect(metadata)

    conn = get_conn(config, expected_slot, role)
    new_commands = [["ASKING"] | commands]

    case pipeline_with_retry(config, role, conn, key_or_keys, new_commands, opts) do
      {:ok, result} ->
        # Remove the ASKING response from the result.
        {:ok, Enum.drop(result, 1)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec hash_slot(:any, Keyword.t()) :: :any
  @spec hash_slot(key() | [key()], Keyword.t()) :: RedisCluster.Key.hash()
  defp hash_slot(:any, _opts) do
    :any
  end

  defp hash_slot([key | _], opts) do
    # Assumes all keys hash to the same slot.
    key |> to_string() |> Key.hash_slot(opts)
  end

  defp hash_slot(key, opts) do
    key |> to_string() |> Key.hash_slot(opts)
  end

  defp select_conn(config, key_or_keys, role_selector, opts) do
    case hash_slot(key_or_keys, opts) do
      :any ->
        random_conn(config, role_selector)

      slot ->
        get_conn(config, slot, role_selector)
    end
  end

  defp random_conn(config, role_selector) do
    slots =
      for {_mod, _lo, _hi, role, host, port} <- HashSlots.all_slots(config),
          role_selector == :any or role == role_selector do
        {host, port}
      end

    {host, port} = Enum.random(slots)

    RedisCluster.Pool.get_conn(config, host, port)
  end

  @spec get_conn(Configuration.t(), slot :: RedisCluster.Key.hash(), role()) :: pid()
  defp get_conn(config, slot, role) do
    {host, port} = lookup(config, slot, role)
    RedisCluster.Pool.get_conn(config, host, port)
  end

  defp lookup(config, slot, role) do
    config
    |> HashSlots.lookup_conn_info(slot, role)
    |> Enum.sort()
    |> case do
      [] ->
        lookup_fallback(config, slot, role)

      [info] ->
        info

      list ->
        pick_consistent(list)
    end
  end

  defp lookup_fallback(_config, slot, role = :master) do
    raise RedisCluster.Exception, message: "No nodes found for slot #{slot} with role #{role}"
  end

  defp lookup_fallback(config, slot, role) do
    # If requesting a replica (or any), fallback to master.
    Logger.warning("No nodes found for slot #{slot} with role #{role}, falling back to master.",
      config_name: config.name
    )

    lookup(config, slot, :master)
  end

  defp pick_consistent([info]) do
    info
  end

  defp pick_consistent(list) do
    # We could have multiple replicas or a mix of master and replicas.
    # Pick the same node for a given process.
    count = length(list)
    index = :erlang.phash2(self(), count)

    Enum.at(list, index)
  end

  defp maybe_rediscover(config, errors) do
    info =
      for %Redix.Error{message: "MOVED" <> rest} <- errors do
        parse_redirect(rest, config.host)
      end

    if info != [] do
      metadata = %{
        info: info,
        errors: errors,
        config_name: config.name,
        table: HashSlots.all_slots_as_table(config)
      }

      Telemetry.moved_redirect(metadata)

      Logger.warning("Some commands failed with MOVED redirect, rediscovering cluster.",
        metadata: metadata
      )

      rediscover(config)
    end

    errors
  end

  defp rediscover(config) do
    RedisCluster.ShardDiscovery.rediscover_shards(config)
  end

  # Format for redirect is `ASK|MOVED <target_slot> [<target_host>]:<port>`.
  # This function assumes the `ASK` or `MOVED` portion has been removed from the message.
  defp parse_redirect(message, original_host) do
    [target_slot, target_host] = String.split(message, " ", parts: 2, trim: true)
    target_slot = String.to_integer(target_slot)

    case target_host do
      ":" <> port ->
        # No domain, just a port.
        # So we use the original host with a different port.
        {target_slot, original_host, String.to_integer(port)}

      "" <> host ->
        [domain, port] = String.split(host, ":", parts: 2, trim: true)
        {target_slot, domain, String.to_integer(port)}
    end
  end
end
