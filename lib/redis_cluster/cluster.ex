defmodule RedisCluster.Cluster do
  alias RedisCluster.Key
  alias RedisCluster.HashSlots
  alias RedisCluster.Configuration

  use Supervisor

  @type key() :: binary() | atom() | number()
  @type value() :: binary() | atom() | number()
  @type command() :: [binary()]
  @type pipeline() :: [command()]

  def start_link(config = %Configuration{}) do
    Supervisor.start_link(__MODULE__, config, name: config.cluster)
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

  @doc """
  Calls the [Redis `GET` command](https://redis.io/commands/get).

  Options:
    * `:compute_hash_tag` - Whether to compute the hash tag of the key (default `false`).
    * `:role` - The role to use when querying the cluster. Possible values are:
      - `:master` - Query the master node.
      - `:replica` - Query a replica node (default).
  """
  @spec get(Configuration.t(), key(), Keyword.t()) :: binary()
  def get(config, key, opts \\ []) do
    key = to_string(key)
    role = Keyword.get(opts, :role, :replica)
    slot = Key.hash_slot(key, opts)

    with_retry(config, role, slot, fn conn ->
      Redix.command(conn, ["GET", key])
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

    with_retry(config, role, slot, fn conn ->
      Redix.command(conn, command)
    end)
    |> case do
      "OK" -> :ok
      error -> error
    end
  end

  @doc """
  Calls the [Redis `DEL` command](https://redis.io/docs/latest/commands/del).

  Returns 1 if the key was deleted, 0 if the key was not found.

  Since this is a write command, it will always target master nodes.

  Options:
    * `:compute_hash_tag` - Whether to compute the hash tag of the key (default `false`).
  """
  @spec delete(Configuration.t(), key(), Keyword.t()) :: integer()
  def delete(config, key, opts \\ []) do
    key = to_string(key)
    role = :master
    slot = Key.hash_slot(key, opts)

    with_retry(config, role, slot, fn conn ->
      Redix.command(conn, ["DEL", key])
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
  @spec set_many(Configuration.t(), [{key(), value()}], Keyword.t()) :: :ok | [{:error, any()}]
  def set_many(config, pairs, opts \\ [])

  def set_many(_config, [], _opts) do
    :ok
  end

  def set_many(config, [{k, v}], opts) do
    opts = Keyword.merge([compute_hash_tag: true], opts)
    set(config, k, v, opts)
  end

  def set_many(config, pairs, opts) do
    opts = Keyword.merge([compute_hash_tag: true], opts)
    role = :master

    pairs_by_conn =
      pairs
      |> Map.new(fn {k, v} -> {to_string(k), v} end)
      |> Enum.group_by(fn {k, _v} ->
        slot = Key.hash_slot(k, opts)
        get_conn(config, slot, role)
      end)

    for {conn, pairs} <- pairs_by_conn do
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
      - `:master` - Query the master node.
      - `:replica` - Query a replica node (default).
  """
  def get_many(config, keys, opts \\ [])

  def get_many(_config, [], _opts) do
    []
  end

  def get_many(config, [key], opts) do
    opts = Keyword.merge([compute_hash_tag: true], opts)
    get(config, key, opts)
  end

  def get_many(config, keys, opts) do
    opts = Keyword.merge([compute_hash_tag: true], opts)
    role = Keyword.get(opts, :role, :replica)

    values_by_key =
      keys
      |> MapSet.new(&to_string/1)
      |> Enum.group_by(&Key.hash_slot(&1, opts))
      |> Enum.flat_map(fn {slot, keys} ->
        values =
          with_retry(config, role, slot, fn conn ->
            case Redix.command(conn, ["MGET" | keys]) do
              {:error, _} -> Enum.map(keys, fn _ -> nil end)
              values -> values
            end
          end)

        Enum.zip(keys, values)
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
  @spec delete_many(Configuration.t(), [key()], Keyword.t()) :: integer()
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
    |> Enum.reject(&match?({:ok, _}, &1))
    |> case do
      [] -> :ok
      errors -> maybe_rediscover(config, errors)
    end
  end

  @doc """
  Sends the given command to Redis. 
  This allows sending any command to Redis that isn't already implemented in this module.

  Options:
    * `:compute_hash_tag` - Whether to compute the hash tag of the key (default `true`).
    * `:key` - (**REQUIRED**) The key to use when determining the hash slot.
    * `:role` - The role to use when querying the cluster. Possible values are:
      - `:master` - Query the master node (master).
      - `:replica` - Query a replica node.
  """
  @spec command(Configuration.t(), command(), Keyword.t()) :: term()
  def command(config, command, opts \\ []) do
    role = Keyword.get(opts, :role, :master)
    key = opts |> Keyword.fetch!(:key) |> to_string()
    slot = Key.hash_slot(key, opts)

    with_retry(config, role, slot, fn conn ->
      Redix.command(conn, command)
    end)
  end

  @doc """
  Sends a sequence of commands to Redis. 
  This allows sending any command to Redis that isn't already implemented in this module.
  It sends the commands in one batch, reducing the number of round trips.

  Options:
    * `:compute_hash_tag` - Whether to compute the hash tag of the key (default `true`).
    * `:key` - (**REQUIRED**) The key to use when determining the hash slot.
    * `:role` - The role to use when querying the cluster. Possible values are:
      - `:master` - Query the master node (master).
      - `:replica` - Query a replica node.
  """
  @spec pipeline(Configuration.t(), pipeline(), Keyword.t()) :: [term()]
  def pipeline(config, commands, opts \\ []) do
    role = Keyword.get(opts, :role, :master)
    key = Keyword.fetch!(opts, :key)
    slot = Key.hash_slot(key, opts)

    with_retry(config, role, slot, fn conn ->
      Redix.pipeline(conn, commands)
    end)
  end

  @doc """
  Sends the given pipeline to all nodes in the cluster.
  May filter on a specific role if desired, defaults to all nodes.
  Note that this sends a pipeline instead of a single command. 
  You can issue as many commands as you like and get the raw results back.
  This is useful for debugging with commands like `DBSIZE` or `INFO USED_MEMORY`.

  Options:
    * `:role` - The role to use when querying the cluster. Possible values are:
      - `:any` - Query any node (default).
      - `:master` - Query the master nodes.
      - `:replica` - Query the replica nodes.
  """
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

  defp with_retry(config, role, slot, fun) do
    conn = get_conn(config, slot, role)

    case fun.(conn) do
      {:ok, result} ->
        result

      # The key wasn't on the expected node.
      # Try rediscovering the cluster.
      {:error, %Redix.Error{message: "MOVED" <> _}} ->
        rediscover(config)
        conn = get_conn(config, slot, role)
        fun.(conn)

      error = {:error, _} ->
        error
    end
  end

  defp get_conn(config, slot, role) do
    {host, port} = lookup(config, slot, role)
    RedisCluster.Pool.get_conn(config, host, port)
  end

  defp lookup(config, slot, role) do
    config
    |> HashSlots.lookup_conn_info(slot, role)
    |> Enum.sort()
    |> pick_consistent()
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
    if Enum.any?(errors, fn
         {:error, %Redix.Error{message: "MOVED" <> _}} -> true
         _ -> false
       end) do
      rediscover(config)
    end

    errors
  end

  defp rediscover(config) do
    RedisCluster.ShardDiscovery.rediscover_shards(config)
  end
end
