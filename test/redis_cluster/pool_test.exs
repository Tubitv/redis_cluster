defmodule RedisCluster.PoolTest do
  use ExUnit.Case, async: true

  alias RedisCluster.Cluster.NodeInfo
  alias RedisCluster.Configuration
  alias RedisCluster.Pool

  defmodule MockRedisModule do
    use GenServer

    def start_link(opts) do
      test_pid = Process.whereis(:pool_test_process)
      GenServer.start_link(__MODULE__, {test_pid, opts})
    end

    def command!(pid, _cmd), do: pid

    @impl true
    def init({test_pid, opts}) do
      if test_pid do
        send(test_pid, {:redis_start_link, opts})
      end

      {:ok, opts}
    end
  end

  setup do
    Process.register(self(), :pool_test_process)

    on_exit(fn ->
      if Process.whereis(:pool_test_process) do
        Process.unregister(:pool_test_process)
      end
    end)

    config = %Configuration{
      host: "localhost",
      port: 6379,
      name: :test_cluster,
      registry: :test_registry,
      cluster: :test_cluster_name,
      pool: :test_pool,
      shard_discovery: :test_discovery,
      pool_size: 2,
      redis_module: MockRedisModule,
      ssl: false,
      ssl_opts: []
    }

    start_supervised!({Registry, keys: :unique, name: :test_registry})
    start_supervised!({DynamicSupervisor, name: :test_pool, strategy: :one_for_one})

    {:ok, config: config}
  end

  describe "start_pool/2 connection options" do
    test "builds connection options without SSL", %{config: config} do
      node_info = %NodeInfo{
        host: "redis-node-1.example.com",
        port: 7000,
        role: :master,
        id: "node1",
        slots: [],
        health: :online
      }

      Pool.start_pool(config, node_info)

      assert_received {:redis_start_link, conn_opts}
      assert conn_opts[:host] == "redis-node-1.example.com"
      assert conn_opts[:port] == 7000
      assert Keyword.has_key?(conn_opts, :name)
      refute Keyword.has_key?(conn_opts, :socket_opts)

      assert_received {:redis_start_link, conn_opts2}
      assert conn_opts2[:host] == "redis-node-1.example.com"
      assert conn_opts2[:port] == 7000
      assert Keyword.has_key?(conn_opts2, :name)
      refute Keyword.has_key?(conn_opts2, :socket_opts)
    end

    test "builds connection options with SSL enabled", %{config: base_config} do
      config = %{
        base_config
        | ssl: true,
          ssl_opts: [verify: :verify_peer, cacertfile: "/path/to/ca.crt"]
      }

      node_info = %NodeInfo{
        host: "secure-redis.example.com",
        port: 6380,
        role: :replica,
        id: "node2",
        slots: [],
        health: :online
      }

      Pool.start_pool(config, node_info)

      assert_received {:redis_start_link, conn_opts}
      assert conn_opts[:host] == "secure-redis.example.com"
      assert conn_opts[:port] == 6380
      assert Keyword.has_key?(conn_opts, :name)

      assert conn_opts[:socket_opts] == [
               ssl: [verify: :verify_peer, cacertfile: "/path/to/ca.crt"]
             ]

      assert_received {:redis_start_link, conn_opts2}
      assert conn_opts2[:host] == "secure-redis.example.com"
      assert conn_opts2[:port] == 6380
      assert Keyword.has_key?(conn_opts2, :name)

      assert conn_opts2[:socket_opts] == [
               ssl: [verify: :verify_peer, cacertfile: "/path/to/ca.crt"]
             ]
    end

    test "builds connection options with comprehensive SSL options", %{config: base_config} do
      config = %{
        base_config
        | ssl: true,
          ssl_opts: [
            verify: :verify_peer,
            cacertfile: "/etc/ssl/certs/ca.pem",
            certfile: "/etc/ssl/certs/client.pem",
            keyfile: "/etc/ssl/private/client.key",
            server_name_indication: ~c"redis.example.com"
          ]
      }

      node_info = %NodeInfo{
        host: "redis-master.example.com",
        port: 6379,
        role: :master,
        id: "master1",
        slots: [],
        health: :online
      }

      Pool.start_pool(config, node_info)

      assert_received {:redis_start_link, conn_opts}
      assert conn_opts[:host] == "redis-master.example.com"
      assert conn_opts[:port] == 6379
      assert Keyword.has_key?(conn_opts, :name)

      assert conn_opts[:socket_opts] == [
               ssl: [
                 verify: :verify_peer,
                 cacertfile: "/etc/ssl/certs/ca.pem",
                 certfile: "/etc/ssl/certs/client.pem",
                 keyfile: "/etc/ssl/private/client.key",
                 server_name_indication: ~c"redis.example.com"
               ]
             ]
    end

    test "preserves separate connection names for pool members", %{config: config} do
      node_info = %NodeInfo{
        host: "localhost",
        port: 6379,
        role: :master,
        id: "node1",
        slots: [],
        health: :online
      }

      Pool.start_pool(config, node_info)

      assert_received {:redis_start_link, conn_opts1}
      name1 = conn_opts1[:name]

      assert_received {:redis_start_link, conn_opts2}
      name2 = conn_opts2[:name]

      refute name1 == name2
    end
  end
end
