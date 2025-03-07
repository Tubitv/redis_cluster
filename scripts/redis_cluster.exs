#!/usr/bin/env elixir

defmodule RedisCluster do
  def create_redis_conf(port) do
    """
    ###############################
    # Redis Cluster Configuration #
    ###############################

    port #{port}

    cluster-enabled yes
    cluster-config-file nodes-#{port}.conf
    cluster-node-timeout 5000

    dir #{System.tmp_dir()}redis/#{port}

    appendonly yes

    protected-mode no
    bind 0.0.0.0

    logfile "#{System.tmp_dir()}redis/#{port}.log"

    dbfilename dump-#{port}.rdb

    save 900 1
    save 300 10
    save 60 10000
    """
  end

  def create_root_dir() do
    for port <- ports() do
      File.mkdir_p("#{System.tmp_dir()}redis/#{port}")
    end
  end

  def ports() do
    7000..7005
  end

  def start_instances() do
    for port <- ports() do
      IO.puts("Starting Redis on port #{port}")
      path = "#{System.tmp_dir()}redis-#{port}.conf"
      File.write!(path, create_redis_conf(port))

      Task.start(fn ->
        System.cmd("redis-server", [path])
      end)
    end
  end

  def cluster() do
    nodes = Enum.map(ports(), fn port -> "127.0.0.1:#{port}" end)

    IO.puts("Creating Redis Cluster with nodes: #{inspect(nodes)}")

    System.cmd(
      "redis-cli",
      ["--cluster", "create", "--cluster-yes"] ++ nodes ++ ["--cluster-replicas", "1"]
    )
  end

  def stop_instances() do
    for port <- ports() do
      IO.puts("Stopping Redis on port #{port}")
      System.cmd("redis-cli", ["-p", "#{port}", "shutdown"])
    end
  end

  def print_help() do
    IO.puts("Usage: start_redis_cluster.exs [start|stop]")
  end
end

case System.argv() do
  ["start" | _] ->
    RedisCluster.create_root_dir()
    RedisCluster.start_instances()
    IO.puts("Waiting for Redis instances to start...")
    Process.sleep(5000)
    RedisCluster.cluster()

  ["stop" | _] ->
    RedisCluster.stop_instances()

  _ ->
    RedisCluster.print_help()
    exit(1)
end
