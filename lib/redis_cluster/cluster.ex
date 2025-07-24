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
      command_with_retry(config, role, slot, key, ["GET", key])
    end)
  end

  @doc """
  Calls the [Redis `SET` command](https://redis.io/commands/set).

  By default doesn't set an expiration time for the key. Only `:expire_seconds` or
  `:expire_milliseconds` can be set, not both.

  Since this is a write command, it will always target master nodes.

  Options:
    * `:compute_hash_tag` - Whether to compute the hash tag of the key (default `false`).
    * `:expire_seconds` - The number of seconds until the key expires.
    * `:expire_milliseconds` - The number of milliseconds until the key expires.
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
      case command_with_retry(config, role, slot, key, command) do
        {:error, _} = error -> error
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
      command_with_retry(config, role, slot, key, ["DEL", key])
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

  Options:
    * `:compute_hash_tag` - Whether to compute the hash tag of the key (default `true`).
    * `:expire_seconds` - The number of seconds until the key expires.
    * `:expire_milliseconds` - The number of milliseconds until the key expires.
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
    role = :master

    # Create a map of conn => [{key, value}, ...]
    pairs_by_conn =
      pairs
      |> Map.new(fn {k, v} -> {to_string(k), v} end)
      |> Enum.group_by(fn {k, _v} ->
        slot = Key.hash_slot(k, opts)
        get_conn(config, slot, role)
      end)

    for {conn, pairs} <- pairs_by_conn do
      # Create a list of SET commands with the requested options.
      cmds =
        Enum.map(pairs, fn {k, v} ->
          List.flatten([
            ["SET", k, v],
            expire_option(opts),
            write_option(opts)
          ])
        end)

      Redix.pipeline(conn, cmds)
    end
    |> Enum.reject(&match?({:ok, _}, &1))
    |> case do
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
      |> Enum.uniq()
      |> Enum.group_by(&Key.hash_slot(&1, opts))
      |> Enum.flat_map(fn {slot, key_batch} ->
        case command_with_retry(config, role, slot, key_batch, ["MGET" | key_batch]) do
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
  **WARNING**: This command is not a one-to-one mapping to the
  [Redis `DEL` command](https://redis.io/docs/latest/commands/del/).
  See the `set_many/3` function for details why.

  Deletes the listed keys and returns the number of keys that were deleted.
  Only unique `DEL` commands are sent.
  If there are duplicate keys, this number deleted will be less than total keys given.

  Since this is a write command, it will always target master nodes.

  Options:
    * `:compute_hash_tag` - Whether to compute the hash tag of the key (default `true`).
  """
  @spec delete_many(Configuration.t(), [key()], Keyword.t()) :: integer() | {:error, any()}
  def delete_many(config, keys, opts \\ [])

  def delete_many(_config, [], _opts) do
    0
  end

  def delete_many(config, [key], opts) do
    delete(config, key, opts)
  end

  def delete_many(config, keys, opts) when is_list(keys) do
    opts = Keyword.merge([compute_hash_tag: true], opts)
    role = :master

    keys_by_conn =
      keys
      |> MapSet.new(&to_string/1)
      |> Enum.group_by(fn key ->
        slot = Key.hash_slot(key, opts)
        get_conn(config, slot, role)
      end)

    for {conn, keys} <- keys_by_conn do
      cmds = Enum.map(keys, &["DEL", &1])

      Redix.pipeline(conn, cmds)
    end
    |> Enum.reduce_while(0, fn
      {:ok, results}, acc ->
        {:cont, acc + Enum.sum(results)}

      error = {:error, %Redix.Error{message: "MOVED" <> rest}}, _acc ->
        # If we get a MOVED error, we need to rediscover the cluster.
        [slot, host] = String.split(rest, " ", parts: 2, trim: true)

        Logger.warning("Received MOVED error with delete_many, rediscovering cluster.",
          host: host,
          slot: slot,
          role: role,
          all_keys: keys,
          config_name: config.name,
          table: HashSlots.all_slots_as_table(config)
        )

        rediscover(config)
        {:halt, error}

      _error, acc ->
        {:cont, acc}
    end)
  end

  @doc """
  Sends the given command to Redis.
  This allows sending any command to Redis that isn't already implemented in this module.

  Options:
    * `:compute_hash_tag` - Whether to compute the hash tag of the key (default `false`). See `RedisCluster.Key.hash_slot/2`.
    * `:key` - (**REQUIRED**) The key to use when determining the hash slot.
    * `:role` - The role to use when querying the cluster. Possible values are:
      - `:master` - Query the master node (default).
      - `:replica` - Query a replica node.
  """
  @spec command(Configuration.t(), command(), Keyword.t()) :: term() | {:error, any()}
  def command(config, command, opts) do
    role = Keyword.get(opts, :role, :master)
    key = opts |> Keyword.fetch!(:key) |> to_string()
    slot = Key.hash_slot(key, opts)

    metadata = %{
      config_name: config.name,
      key: key,
      role: role,
      slot: slot
    }

    Telemetry.execute_command(command, metadata, fn ->
      command_with_retry(config, role, slot, key, command)
    end)
  end

  @doc """
  Sends a sequence of commands to Redis.
  This allows sending any command to Redis that isn't already implemented in this module.
  It sends the commands in one batch, reducing the number of round trips.

  Responses are returned as a list in the same order as the commands.

  Options:
    * `:compute_hash_tag` - Whether to compute the hash tag of the key (default `false`). See `RedisCluster.Key.hash_slot/2`.
    * `:key` - (**REQUIRED**) The key to use when determining the hash slot.
    * `:role` - The role to use when querying the cluster. Possible values are:
      - `:master` - Query the master node (default).
      - `:replica` - Query a replica node.
  """
  @spec pipeline(Configuration.t(), pipeline(), Keyword.t()) :: [term()] | {:error, any()}
  def pipeline(config, commands, opts) do
    role = Keyword.get(opts, :role, :master)
    key = Keyword.fetch!(opts, :key)
    slot = Key.hash_slot(key, opts)

    metadata = %{
      config_name: config.name,
      key: key,
      role: role,
      slot: slot
    }

    Telemetry.execute_pipeline(commands, metadata, fn ->
      case pipeline_with_retry(config, role, slot, key, commands) do
        {:ok, results} -> results
        {:error, reason} -> {:error, reason}
      end
    end)
  end

  @spec broadcast(RedisCluster.Configuration.t(), [[binary()]]) :: [
          {binary(), non_neg_integer(), any()}
        ]
  @doc """
  Sends the given pipeline to all nodes in the cluster.
  May filter on a specific role if desired, defaults to all nodes.

  Note that this sends a pipeline instead of a single command.
  You can issue as many commands as you like and get the raw results back.
  The pipelines are sent to each node sequentially, so this may take some time.
  This is useful for debugging with commands like `DBSIZE` or `INFO MEMORY`.
  Be sure to only send commands that are safe to run on any node.

  Options:
    * `:role` - The role to use when querying the cluster. Possible values are:
      - `:any` - Query any node (default).
      - `:master` - Query the master nodes.
      - `:replica` - Query the replica nodes.
  """
  @spec broadcast(Configuration.t(), pipeline(), Keyword.t()) ::
          [{host :: String.t(), port :: non_neg_integer(), result :: any()}]
  def broadcast(config, commands, opts \\ []) do
    role_selector = Keyword.get(opts, :role, :any)

    for {_mod, _lo, _hi, role, host, port} <- HashSlots.all_slots(config),
        role_selector == :any or role == role_selector do
      conn = RedisCluster.Pool.get_conn(config, host, port)
      result = Redix.pipeline(conn, commands)

      {host, port, result}
    end
  end

  ## Helpers

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
          slot :: RedisCluster.Key.hash(),
          key() | [key()],
          command()
        ) :: Redix.Protocol.redis_value() | {:error, any()}
  defp command_with_retry(config, role, slot, key, command) do
    conn = get_conn(config, slot, role)

    case pipeline_with_retry(config, role, slot, conn, key, [command]) do
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
          slot :: RedisCluster.Key.hash(),
          key() | [key()],
          commands :: pipeline()
        ) :: {:ok, [Redix.Protocol.redis_value()]} | {:error, any()}
  defp pipeline_with_retry(config, role, slot, key_or_keys, commands) do
    conn = get_conn(config, slot, role)

    pipeline_with_retry(config, role, slot, conn, key_or_keys, commands)
  end

  @spec pipeline_with_retry(
          Configuration.t(),
          role(),
          slot :: RedisCluster.Key.hash(),
          conn :: pid(),
          key_or_keys :: key() | [key()],
          commands :: pipeline()
        ) :: {:ok, [Redix.Protocol.redis_value()]} | {:error, any()}
  defp pipeline_with_retry(config, role, slot, conn, key_or_keys, commands) do
    case Redix.pipeline(conn, commands) do
      {:ok, result} ->
        {:ok, result}

      # The key wasn't on the expected node.
      # Try rediscovering the cluster.
      {:error, %Redix.Error{message: "MOVED " <> rest}} ->
        handle_moved_redirect(config, role, slot, key_or_keys, commands, rest)

      # A temporary redirect.
      {:error, %Redix.Error{message: "ASK " <> rest}} ->
        handle_ask_redirect(config, role, slot, key_or_keys, commands, rest)

      error = {:error, reason} ->
        Logger.warning("Failed to query Redis",
          slot: slot,
          role: role,
          keys: List.wrap(key_or_keys),
          config_name: config.name,
          reason: reason
        )

        error
    end
  end

  defp handle_moved_redirect(config, role, slot, key_or_keys, commands, rest) do
    {expected_slot, host, port} = parse_redirect(rest, config.host)
    expected_host = "#{host}:#{port}"

    metadata = %{
      slot: slot,
      role: role,
      expected_slot: expected_slot,
      expected_host: expected_host,
      keys: List.wrap(key_or_keys),
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
    Redix.pipeline(conn, commands)
  end

  defp handle_ask_redirect(config, role, slot, key_or_keys, commands, rest) do
    {expected_slot, domain, port} = parse_redirect(rest, config.host)
    expected_host = "#{domain}:#{port}"

    metadata = %{
      slot: slot,
      role: role,
      expected_slot: expected_slot,
      expected_host: expected_host,
      keys: List.wrap(key_or_keys),
      config_name: config.name
    }

    Logger.warning("Received ASK redirect",
      metadata: metadata,
      table: HashSlots.all_slots_as_table(config)
    )

    Telemetry.ask_redirect(metadata)

    conn = get_conn(config, expected_slot, role)
    new_commands = [["ASKING"] | commands]

    case pipeline_with_retry(config, role, expected_slot, conn, key_or_keys, new_commands) do
      {:ok, result} ->
        # Remove the ASKING response from the result.
        {:ok, Enum.drop(result, 1)}

      {:error, reason} ->
        {:error, reason}
    end
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
        String.split(rest, " ", parts: 2, trim: true)
      end

    if info != [] do
      Logger.warning("Some commands failed, rediscovering cluster.",
        info: info,
        config_name: config.name,
        table: HashSlots.all_slots_as_table(config)
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
