defmodule RedisCluster.Configuration do
  # TODO: Grab the configuration from the environment
  # This includes the host, port, registry, pool size. 

  # The host should ideally be the configuration endpoint for ElastiCache (or equivalent).
  # The configuration endpoint picks a random node in the cluster to connect to.
  # This avoids having all the discovery traffic go to a single node.
  defstruct [:host, :port, :name, :registry, :pool_size]

  def from_app_env(opts, module) do
    otp_app = Keyword.fetch!(opts, :otp_app)
    name = Keyword.fetch!(opts, :name) || module
    env = Application.get_env(otp_app, name)

    %__MODULE__{
      host: Keyword.fetch!(env, :host),
      port: Keyword.fetch!(env, :port),
      name: name,
      registry: Keyword.get(env, :registry) || Module.concat([name, "Registry__"]),
      pool_size: Keyword.get(env, :pool_size, 10)
    }
  end
end
