defmodule RedisCluster.Configuration do
  # This includes the host, port, registry, pool size. 

  # The host should ideally be the configuration endpoint for ElastiCache (or equivalent).
  # The configuration endpoint picks a random node in the cluster to connect to.
  # This avoids having all the discovery traffic go to a single node.
  @enforce_keys [:host, :port, :name, :registry, :cluster, :pool, :shard_discovery, :pool_size]
  defstruct [:host, :port, :name, :registry, :cluster, :pool, :shard_discovery, :pool_size]

  @type t :: %__MODULE__{
          host: String.t(),
          port: non_neg_integer(),
          name: atom(),
          registry: atom(),
          cluster: atom(),
          pool: atom(),
          shard_discovery: atom(),
          pool_size: non_neg_integer()
        }

  @spec from_app_env(Keyword.t(), atom()) :: t()
  def from_app_env(opts, module) do
    otp_app = Keyword.fetch!(opts, :otp_app)
    name = Keyword.get(opts, :name) || module
    env = env(otp_app, name, opts)

    %__MODULE__{
      host: Keyword.fetch!(env, :host),
      port: Keyword.fetch!(env, :port),
      name: name,
      registry: fetch_name(name, env, :registry, "Registry__"),
      cluster: fetch_name(name, env, :cluster, "Cluster__"),
      pool: fetch_name(name, env, :pool, "Pool__"),
      shard_discovery: fetch_name(name, env, :shard_discovery, "ShardDiscovery__"),
      pool_size: Keyword.get(env, :pool_size, 10)
    }
  end

  defp env(:none, _name, opts) do
    opts
  end

  defp env(otp_app, name, _opts) do
    case Application.get_env(otp_app, name) do
      nil ->
        raise ArgumentError,
              """
              RedisCluster configuration not found for #{inspect(name)} in app #{inspect(otp_app)}
              Please ensure you have something like the following in your config:

                  config :#{otp_app}, #{inspect(name)},
                    host: System.get_env("REDIS_CLUSTER_URL", "localhost"),
                    port: System.get_env("REDIS_CLUSTER_PORT", "6379") |> String.to_integer(),
                    pool_size: 10
              """

      opts ->
        opts
    end
  end

  defp fetch_name(name, env, key, suffix) do
    Keyword.get(env, key) || Module.concat([name, suffix])
  end
end
