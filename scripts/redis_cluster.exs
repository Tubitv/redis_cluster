#!/usr/bin/env elixir

defmodule Printer do
  def print(color \\ IO.ANSI.default_color(), message) do
    IO.puts([color, message, IO.ANSI.reset()])
  end

  def error(color \\ IO.ANSI.red(), message) do
    IO.puts(:stderr, [color, message, IO.ANSI.reset()])
  end
end

defmodule Shell do
  def run([cmd | args], opts) do
    if Keyword.get(opts, :dry_run, false) do
      Printer.print(IO.ANSI.yellow(), "DRY RUN: #{cmd} #{Enum.join(args, " ")}")
    else
      System.cmd(cmd, args)
    end
  end

  def run(string, opts) when is_binary(string) do
    string
    |> String.split()
    |> run(opts)
  end

  def tmp_dir() do
    dir = System.tmp_dir()

    if String.ends_with?(dir, "/") do
      dir
    else
      dir <> "/"
    end
  end
end

defmodule CLI do
  def main(argv) do
    argv
    |> parse_subcommand()
    |> run()
  end

  defp parse_subcommand(["start" | args]) do
    {:start, parse_options(args)}
  end

  defp parse_subcommand(["standalone" | args]) do
    {:standalone, parse_options(args)}
  end

  defp parse_subcommand(["stop" | args]) do
    {ports, args} = parse_ports(args, [])
    {:stop, ports, parse_options(args)}
  end

  defp parse_subcommand([help | _args]) when help in ["help", "-h", "--help"] do
    print_help()
    halt()
  end

  defp parse_subcommand(args) do
    Printer.error("Unsupported command: #{inspect(args)}")
    print_help()
    halt(1)
  end

  defp parse_options(argv) do
    argv
    |> OptionParser.parse(
      strict: [
        replicas_per_master: :integer,
        port: :integer,
        help: :boolean,
        dry_run: :boolean,
        purge_files: :boolean
      ],
      aliases: [
        r: :replicas_per_master,
        p: :port,
        h: :help,
        d: :dry_run
      ]
    )
    |> process_options()
    |> inject_defaults()
  end

  defp process_options({parsed, [], []}) do
    parsed
  end

  defp process_options({_parsed, [], invalid}) do
    Printer.error("Invalid options: #{inspect(invalid)}")
    print_help()
    halt(2)
  end

  defp process_options({_parsed, argv, _invalid}) do
    Printer.error("Extra arguments: #{inspect(argv)}")
    print_help()
    halt(3)
  end

  defp inject_defaults(opts) do
    Keyword.merge([port: 9000, replicas_per_master: 1, dry_run: false], opts)
  end

  defp run({:start, opts}) do
    maybe_print_help(opts)

    replicas_per_master = Keyword.get(opts, :replicas_per_master, 1)

    if replicas_per_master not in [0, 1, 2, 3, 5] do
      Printer.error("Unsupported replicas per master: #{replicas_per_master}")
      print_help()
      halt(4)
    end

    RedisCluster.start_cluster(replicas_per_master, ports(opts), opts)
  end

  defp run({:standalone, opts}) do
    maybe_print_help(opts)
    port = opts |> ports() |> Enum.at(0)
    replicas_per_master = Keyword.get(opts, :replicas_per_master, 0)

    RedisCluster.start_standalone(replicas_per_master, port, opts)
  end

  defp run({:stop, [], opts}) do
    run({:stop, ports(opts), opts})
  end

  defp run({:stop, ports, opts}) do
    maybe_print_help(opts)
    RedisCluster.stop_instances(ports, opts)

    if Keyword.get(opts, :purge_files, false) do
      purge_files(ports, opts)
    end
  end

  defp maybe_print_help(opts) do
    if Keyword.get(opts, :help, false) do
      print_help()
      halt()
    end
  end

  defp print_help() do
    Printer.print("""
    Usage:
      start_redis_cluster.exs start [options]
      start_redis_cluster.exs single [options]
      start_redis_cluster.exs stop <port> <port> ... [options]

    Options:
      -r, --replicas-per-master <n>  Number of replicas per master (default: 1)
      -p, --port <port>              First port to use. Will use the next 11 ports too. (default: 9000)
      -d, --dry-run                  Print commands instead of running them
      -h, --help                     Print this help message
    """)
  end

  defp parse_ports([], result) do
    {Enum.reverse(result), []}
  end

  defp parse_ports(opts = ["-" <> _ | _], result) do
    {Enum.reverse(result), opts}
  end

  defp parse_ports([string | rest], result) do
    parse_ports(rest, [String.to_integer(string) | result])
  end

  defp ports(opts) do
    port = Keyword.fetch!(opts, :port)

    port..(port + 11)
  end

  defp purge_files(ports, opts) do
    for port <- ports do
      Printer.print("Purging files for Redis on port #{port}")
      Shell.run("rm -rf #{Shell.tmp_dir()}redis/#{port}", opts)
      Shell.run("rm -rf #{Shell.tmp_dir()}redis/#{port}.log", opts)
    end
  end

  defp halt() do
    exit(:normal)
  end

  defp halt(n) when is_integer(n) do
    exit({:shutdown, n})
  end
end

defmodule RedisCluster do
  def start_cluster(replicas_per_master, ports, opts) do
    create_root_dir(ports, opts)
    start_instances(ports, opts)
    Printer.print("Waiting for Redis instances to start...")
    Process.sleep(5000)
    cluster(replicas_per_master, ports, opts)
  end

  def start_standalone(replicas_per_master, port, opts) do
    Printer.print("Starting standalone master #{port}")

    Shell.run("redis-server --port #{port} --daemonize yes", opts)

    if replicas_per_master > 0 do
      for n <- 1..replicas_per_master do
        master_port = port
        replica_port = master_port + n

        Printer.print("Starting standalone replica #{replica_port}")

        Shell.run(
          "redis-server --port #{replica_port} --replicaof localhost #{master_port} --daemonize yes",
          opts
        )
      end
    end
  end

  def stop_instances(ports, opts) do
    for port <- ports do
      Printer.print("Stopping Redis on port #{port}")
      Shell.run("redis-cli -p #{port} shutdown", opts)
    end
  end

  ## Helpers

  defp create_redis_conf(port) do
    """
    ###############################
    # Redis Cluster Configuration #
    ###############################

    port #{port}

    cluster-enabled yes
    cluster-config-file nodes-#{port}.conf
    cluster-node-timeout 100

    dir #{Shell.tmp_dir()}redis/#{port}

    appendonly yes

    protected-mode no
    bind 0.0.0.0

    logfile "#{Shell.tmp_dir()}redis/#{port}.log"

    dbfilename dump-#{port}.rdb

    save 900 1
    save 300 10
    save 60 10000
    """
  end

  defp create_root_dir(ports, opts) do
    for port <- ports do
      Shell.run("mkdir -p #{Shell.tmp_dir()}redis/#{port}", opts)
    end
  end

  defp start_instances(ports, opts) do
    for port <- ports do
      Printer.print("Starting Redis on port #{port}")
      path = "#{Shell.tmp_dir()}redis-#{port}.conf"
      File.write!(path, create_redis_conf(port))

      Task.start(fn ->
        Shell.run(["redis-server", path], opts)
      end)
    end
  end

  defp cluster(replicas_per_master, ports, opts) do
    nodes = Enum.map(ports, fn port -> "127.0.0.1:#{port}" end)

    Printer.print("Creating Redis Cluster with nodes: #{inspect(nodes)}")
    replicas = to_string(replicas_per_master)

    command =
      [
        "redis-cli",
        "--cluster",
        "create",
        "--cluster-yes"
      ] ++ nodes ++ ["--cluster-replicas", replicas]

    Shell.run(command, opts)
  end
end

CLI.main(System.argv())
