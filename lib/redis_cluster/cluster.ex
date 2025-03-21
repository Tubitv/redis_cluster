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

  Since this is a write command, it will always be sent to a master node.

  Options:
    * `:compute_hash_tag` - Whether to compute the hash tag of the key (default `false`).
    * `:expire_seconds` - The number of seconds until the key expires.
    * `:expire_milliseconds` - The number of milliseconds until the key expires.
    * `:set` - Controls when the key should be set. Possible values are:
      - `:always` - Always set the key (default).
      - `:only_overwrite` - Only set the key if it already exists.
      - `:only_new` - Only set the key if it doesn't exist.
  """
  @spec set(Configuration.t(), key(), value(), Keyword.t()) :: binary()
  def set(config, key, value, opts \\ []) do
    key = to_string(key)
    role = :master
    slot = Key.hash_slot(key, opts)

    command =
      List.flatten([
        ["SET", key, value],
        expire_options(opts),
        set_option(opts)
      ])

    with_retry(config, role, slot, fn conn ->
      Redix.command(conn, command)
    end)
  end

  @spec del(Configuration.t(), [key()], Keyword.t()) :: integer()
  def del(config, key_or_keys, opts \\ [])

  def del(config, [key], opts) do
    del(config, key, opts)
  end

  def del(config, keys, opts) when is_list(keys) do
    opts = Keyword.merge([compute_hash_tag: true], opts)

    keys
    |> Enum.group_by(&Key.hash_slot(to_string(&1), opts), &to_string/1)
    |> Enum.reduce(0, fn {slot, keys}, acc ->
      config
      |> with_retry(:master, slot, fn conn ->
        Redix.command(conn, ["DEL", keys])
      end)
      |> case do
        n when is_integer(n) -> acc + n
        _error -> acc
      end
    end)
  end

  @spec del(Configuration.t(), key(), Keyword.t()) :: integer()
  def del(config, key, opts) do
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

  This function is aware of the limitations of the `MSET` command.
  It will group the keys by slot and send the commands to the correct nodes.
  These requests are made sequentially, one per slot, for convenience.
  This means it may be slower than calling several `SET` commands in parallel.

  Since this is a write command, it will always be sent to the master nodes.

  This function cannot guarantee which value is set when a key is included 
  multiple times in one call.

  Options:
    * `:compute_hash_tag` - Whether to compute the hash tag of the key (default `true`).
  """
  @spec set_many(Configuration.t(), [{key(), value()}], Keyword.t()) :: :ok | [{:error, any()}]
  def mset(config, pairs, opts \\ []) do
    opts = Keyword.merge([compute_hash_tag: true], opts)

    pairs_by_slot =
      pairs
      |> Map.new(fn {k, v} -> {to_string(k), v} end)
      |> Enum.group_by(fn {k, _v} -> Key.hash_slot(k, opts) end)

    for {slot, pairs} <- pairs_by_slot do
      keys_and_values = Enum.reduce(pairs, [], fn {k, v}, acc -> [k, v | acc] end)

      cmd = ["MSET" | keys_and_values]

      with_retry(config, :master, slot, fn conn ->
        Redix.command(conn, cmd)
      end)
    end
    |> Enum.reject(&(&1 == "OK"))
    |> case do
      [] -> :ok
      errors -> errors
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
  def get_many(config, keys, opts \\ []) do
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

  @spec command(Configuration.t(), command(), Keyword.t()) :: term()
  def command(config, command, opts \\ []) do
    role = Keyword.get(opts, :role, :master)
    key = opts |> Keyword.fetch!(:key) |> to_string()
    slot = Key.hash_slot(key, opts)

    with_retry(config, role, slot, fn conn ->
      Redix.command(conn, command)
    end)
  end

  @spec pipeline(Configuration.t(), pipeline(), Keyword.t()) :: [term()]
  def pipeline(config, commands, opts \\ []) do
    role = Keyword.get(opts, :role, :master)
    key = Keyword.fetch!(opts, :key)
    slot = Key.hash_slot(key, opts)

    with_retry(config, role, slot, fn conn ->
      Redix.pipeline(conn, commands)
    end)
  end

  ## Helpers

  defp expire_options(opts) do
    expire_seconds = Keyword.get(opts, :expire_seconds, 0)
    expire_milliseconds = Keyword.get(opts, :expire_milliseconds, 0)

    cond do
      expire_milliseconds > 0 -> ["PX", expire_milliseconds]
      expire_seconds > 0 -> ["EX", expire_seconds]
      true -> []
    end
  end

  defp set_option(opts) do
    case Keyword.get(opts, :set, :always) do
      :always -> []
      :only_overwrite -> ["XX"]
      :only_new -> ["NX"]
    end
  end

  defp with_retry(config, role, slot, fun) do
    {host, port} = lookup(config, slot, role)
    conn = RedisCluster.Pool.get_conn(config, host, port)

    case fun.(conn) do
      {:ok, result} ->
        result

      # The key wasn't on the expected node.
      # Try rediscovering the cluster.
      {:error, %Redix.Error{message: "MOVED" <> _}} ->
        reshard(config)
        {host, port} = lookup(config, slot, role)
        conn = RedisCluster.Pool.get_conn(config, host, port)
        fun.(conn)

      error = {:error, _} ->
        error
    end
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

  defp reshard(config) do
    RedisCluster.ShardDiscovery.rediscover_shards(config)
  end
end
