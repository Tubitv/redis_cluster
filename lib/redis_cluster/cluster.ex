defmodule RedisCluster.Cluster do
  alias RedisCluster.Key
  alias RedisCluster.HashSlots
  alias RedisCluster.Configuration

  use Supervisor

  def start_link(config = %Configuration{}) do
    Supervisor.start_link(__MODULE__, config, name: config.cluster)
  end

  def init(config) do
    children = [
      {Registry, keys: :unique, name: config.registry},
      {RedisCluster.Pool, config},
      {RedisCluster.ShardDiscovery, config}
    ]

    # This could probably be :rest_for_one

    Supervisor.init(children, strategy: :one_for_all)
  end

  def get(config, key, opts \\ []) do
    role = Keyword.get(opts, :role, :replica)
    slot = Key.hash_slot(key, opts)

    with_retry(config, role, slot, fn conn ->
      Redix.command(conn, ["GET", key])
    end)
  end

  def set(config, key, value, opts \\ []) do
    role = :master
    slot = Key.hash_slot(key, opts)

    with_retry(config, role, slot, fn conn ->
      Redix.command(conn, ["SET", key, value])
    end)
  end

  def command(config, command, opts \\ []) do
    role = Keyword.get(opts, :role, :replica)
    key = Keyword.fetch!(opts, :key)
    slot = Key.hash_slot(key, opts)

    with_retry(config, role, slot, fn conn ->
      Redix.command(conn, command)
    end)
  end

  def pipeline(config, commands, opts \\ []) do
    role = Keyword.get(opts, :role, :replica)
    key = Keyword.fetch!(opts, :key)
    slot = Key.hash_slot(key, opts)

    with_retry(config, role, slot, fn conn ->
      Redix.pipeline(conn, commands)
    end)
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

  defp lookup(config, slots, role) do
    config.name
    |> HashSlots.lookup_conn_info(slots, role)
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
