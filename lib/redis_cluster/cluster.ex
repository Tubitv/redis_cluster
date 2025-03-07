defmodule RedisCluster.Cluster do
  alias RedisCluster.Key
  alias RedisCluster.HashSlots
  alias RedisCluster.Configuration

  use Supervisor

  def start_link(config = %Configuration{}) do
    Supervisor.start_link(__MODULE__, config, name: name(config))
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

  def name(config) do
    Module.concat(config.name, "Cluster__")
  end

  def get(config, key, opts \\ []) do
    role = Keyword.get(opts, :role, :replica)
    slot = Key.hash_slot(key, opts)
    slot_id = HashSlots.lookup(config.name, slot)

    with_retry(config, role, slot_id, fn conn ->
      Redix.command(conn, ["GET", key])
    end)
  end

  def set(config, key, value, opts \\ []) do
    role = :master
    slot = Key.hash_slot(key, opts)
    slot_id = HashSlots.lookup(config.name, slot)

    with_retry(config, role, slot_id, fn conn ->
      Redix.command(conn, ["SET", key, value])
    end)
  end

  def command(config, command, opts \\ []) do
    role = Keyword.get(opts, :role, :replica)
    key = Keyword.fetch!(opts, :key)
    slot = Key.hash_slot(key, opts)
    slot_id = HashSlots.lookup(config.name, slot)

    with_retry(config, role, slot_id, fn conn ->
      Redix.command(conn, command)
    end)
  end

  def pipeline(config, commands, opts \\ []) do
    role = Keyword.get(opts, :role, :replica)
    key = Keyword.fetch!(opts, :key)
    slot = Key.hash_slot(key, opts)
    slot_id = HashSlots.lookup(config.name, slot)

    with_retry(config, role, slot_id, fn conn ->
      Redix.pipeline(conn, commands)
    end)
  end

  defp with_retry(config, role, slot_id, fun) do
    conn = RedisCluster.Pool.get_conn(config, role, slot_id)

    case fun.(conn) do
      {:ok, result} ->
        result

      # The key wasn't on the expected node.
      # Try rediscovering the cluster.
      {:error, %Redix.Error{message: "MOVED" <> _}} ->
        reshard(config)
        conn = RedisCluster.Pool.get_conn(config, role, slot_id)
        fun.(conn)

      error = {:error, _} ->
        error
    end
  end

  defp reshard(config) do
    RedisCluster.ShardDiscovery.rediscover_shards(config)
  end
end
