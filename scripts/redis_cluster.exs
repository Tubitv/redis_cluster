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
    9000..9011
  end

  def start_cluster(replicas_per_master) do
    RedisCluster.create_root_dir()
    RedisCluster.start_instances()
    IO.puts("Waiting for Redis instances to start...")
    Process.sleep(5000)
    RedisCluster.cluster(replicas_per_master)
  end

  def start_instances() do
    for port <- ports() do
      IO.puts("Starting Redis on port #{port}")
      path = "#{System.tmp_dir()}redis-#{port}.conf"
      File.write!(path, create_redis_conf(port))

      Task.start(fn ->
        run_command(["redis-server", path])
      end)
    end
  end

  def cluster(replicas_per_master) do
    nodes = Enum.map(ports(), fn port -> "127.0.0.1:#{port}" end)

    IO.puts("Creating Redis Cluster with nodes: #{inspect(nodes)}")
    replicas = to_string(replicas_per_master)

    command =
      [
        "redis-cli",
        "--cluster",
        "create",
        "--cluster-yes"
      ] ++ nodes ++ ["--cluster-replicas", replicas]

    run_command(command)
  end

  def stop_instances(ports) do
    for port <- ports do
      IO.puts("Stopping Redis on port #{port}")
      run_command(["redis-cli", "-p", "#{port}", "shutdown"])
    end
  end

  def print_help() do
    IO.puts("""
    Usage: 
    start_redis_cluster.exs start
    start_redis_cluster.exs start [0|1|2|3|5]
    start_redis_cluster.exs stop
    start_redis_cluster.exs stop <port> <port> ...
    """)
  end

  def parse_ports(ports) do
    Enum.map(ports, &String.to_integer/1)
  end

  def run_command([cmd | args], opts \\ []) do
    if Keyword.get(opts, :dry_run, false) do
      IO.puts("DRY RUN: #{cmd} #{Enum.join(args, " ")}")
    else
      System.cmd(cmd, args)
    end
  end
end

case System.argv() do
  ["start"] ->
    RedisCluster.start_cluster("1")

  ["start", replicas_per_master | _] when replicas_per_master in ~w[0 1 2 3 5] ->
    RedisCluster.start_cluster(replicas_per_master)

  ["start" | replicas_per_master] ->
    IO.puts("Unsupported replicas per master: #{replicas_per_master}")
    RedisCluster.print_help()
    exit(1)

  ["stop"] ->
    RedisCluster.stop_instances(RedisCluster.ports())

  ["stop" | ports] ->
    ports
    |> RedisCluster.parse_ports()
    |> RedisCluster.stop_instances()

  _ ->
    RedisCluster.print_help()
    exit(1)
end
