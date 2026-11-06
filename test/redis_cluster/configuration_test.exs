defmodule RedisCluster.ConfigurationTest do
  use ExUnit.Case, async: true

  alias RedisCluster.Configuration

  defp config(overrides) do
    struct!(
      Configuration,
      Keyword.merge(
        [
          host: "redis.example.com",
          port: 6379,
          name: :cfg,
          registry: :cfg_registry,
          cluster: :cfg_cluster,
          pool: :cfg_pool,
          shard_discovery: :cfg_discovery,
          pool_size: 2
        ],
        overrides
      )
    )
  end

  describe "redix_opts/1" do
    test "has no TLS options when ssl is off" do
      assert Configuration.redix_opts(config([])) == [host: "redis.example.com", port: 6379]
    end

    test "sets Redix ssl: true and passes ssl_opts as socket_opts (not nested under :ssl)" do
      tls = [verify: :verify_peer, server_name_indication: ~c"redis.example.com"]
      opts = Configuration.redix_opts(config(ssl: true, ssl_opts: tls))

      assert opts[:ssl] == true
      assert opts[:socket_opts] == tls
    end

    test "ssl: true with no ssl_opts still enables TLS" do
      opts = Configuration.redix_opts(config(ssl: true))
      assert opts[:ssl] == true
      assert opts[:socket_opts] == []
    end
  end
end
