defmodule Livebook.SmartCell.RedisCluster.Connect do
  use Kino.JS, assets_path: "assets"
  use Kino.SmartCell, name: "RedisCluster: Connect"

  @impl true
  def init(_attrs, _ctx) do
    fields = %{
      name: "MyRedisCluster",
      host: "localhost",
      port: "6379",
      pool_size: "10"
    }

    {:ok, fields, fields}
  end

  @impl true
  def to_source(%{name: name, host: host, port: port, pool_size: pool_size}) do
    cluster_name = name |> String.trim() |> String.to_atom()

    code = """
    name = #{inspect(name)}

    config = %RedisCluster.Configuration{
      name: String.to_atom(name),
      host: #{inspect(host)},
      port: #{port},
      pool_size: #{pool_size},
      registry: Module.concat([name, Registry]),
      pool: Module.concat([name, Pool]),
      cluster: Module.concat([name, Cluster]),
      shard_discovery: Module.concat([name, ShardDiscovery])
    }

    {:ok, _pid} = RedisCluster.Cluster.start_link(config)
    config
    """

    {:ok, code}
  end

  @impl true
  def scan_binding(binding, _ctx), do: Map.new(binding)

  @impl true
  def scan_ast(ast, _ctx), do: %{}
end
