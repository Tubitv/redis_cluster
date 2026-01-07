defmodule RedisCluster.ClusterInfoTest do
  use ExUnit.Case, async: true

  alias RedisCluster.ClusterInfo
  alias RedisCluster.Configuration

  defmodule MockRedisModule do
    use GenServer

    def start_link(opts) do
      parent = self()
      {:ok, pid} = GenServer.start_link(__MODULE__, parent)
      send(parent, {:start_link_called, opts})
      {:ok, pid}
    end

    def command(_conn, _cmd) do
      {:error, %Redix.Error{message: "Mock error"}}
    end

    @impl true
    def init(parent) do
      {:ok, parent}
    end

    @impl true
    def handle_call(_msg, _from, state) do
      {:reply, :ok, state}
    end
  end

  describe "query/1 connection options" do
    test "builds connection options without SSL" do
      config = %Configuration{
        host: "redis.example.com",
        port: 6379,
        name: :test_cluster,
        registry: :test_registry,
        cluster: :test_cluster_name,
        pool: :test_pool,
        shard_discovery: :test_discovery,
        pool_size: 10,
        redis_module: MockRedisModule,
        ssl: false,
        ssl_opts: []
      }

      ClusterInfo.query(config)

      assert_received {:start_link_called, opts}
      assert opts[:host] == "redis.example.com"
      assert opts[:port] == 6379
      refute Keyword.has_key?(opts, :socket_opts)
    end

    test "builds connection options with SSL enabled" do
      config = %Configuration{
        host: "redis.example.com",
        port: 6380,
        name: :test_cluster,
        registry: :test_registry,
        cluster: :test_cluster_name,
        pool: :test_pool,
        shard_discovery: :test_discovery,
        pool_size: 10,
        redis_module: MockRedisModule,
        ssl: true,
        ssl_opts: [verify: :verify_peer, cacertfile: "/path/to/ca.crt"]
      }

      ClusterInfo.query(config)

      assert_received {:start_link_called, opts}
      assert opts[:host] == "redis.example.com"
      assert opts[:port] == 6380
      assert opts[:socket_opts] == [ssl: [verify: :verify_peer, cacertfile: "/path/to/ca.crt"]]
    end

    test "builds connection options with SSL and empty ssl_opts" do
      config = %Configuration{
        host: "localhost",
        port: 7000,
        name: :test_cluster,
        registry: :test_registry,
        cluster: :test_cluster_name,
        pool: :test_pool,
        shard_discovery: :test_discovery,
        pool_size: 10,
        redis_module: MockRedisModule,
        ssl: true,
        ssl_opts: []
      }

      ClusterInfo.query(config)

      assert_received {:start_link_called, opts}
      assert opts[:host] == "localhost"
      assert opts[:port] == 7000
      assert opts[:socket_opts] == [ssl: []]
    end
  end

  describe "query_while/2 connection options" do
    test "builds connection options without SSL" do
      config = %Configuration{
        host: "redis.example.com",
        port: 6379,
        name: :test_cluster,
        registry: :test_registry,
        cluster: :test_cluster_name,
        pool: :test_pool,
        shard_discovery: :test_discovery,
        pool_size: 10,
        redis_module: MockRedisModule,
        ssl: false,
        ssl_opts: []
      }

      ClusterInfo.query_while(config, fn _info, _attempt -> false end)

      assert_received {:start_link_called, opts}
      assert opts[:host] == "redis.example.com"
      assert opts[:port] == 6379
      refute Keyword.has_key?(opts, :socket_opts)
    end

    test "builds connection options with SSL enabled" do
      config = %Configuration{
        host: "secure-redis.example.com",
        port: 6380,
        name: :test_cluster,
        registry: :test_registry,
        cluster: :test_cluster_name,
        pool: :test_pool,
        shard_discovery: :test_discovery,
        pool_size: 10,
        redis_module: MockRedisModule,
        ssl: true,
        ssl_opts: [
          verify: :verify_peer,
          cacertfile: "/path/to/ca.crt",
          certfile: "/path/to/client.crt",
          keyfile: "/path/to/client.key"
        ]
      }

      ClusterInfo.query_while(config, fn _info, _attempt -> false end)

      assert_received {:start_link_called, opts}
      assert opts[:host] == "secure-redis.example.com"
      assert opts[:port] == 6380

      assert opts[:socket_opts] == [
               ssl: [
                 verify: :verify_peer,
                 cacertfile: "/path/to/ca.crt",
                 certfile: "/path/to/client.crt",
                 keyfile: "/path/to/client.key"
               ]
             ]
    end
  end
end
