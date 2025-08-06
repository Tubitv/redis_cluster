defmodule Livebook.SmartCell.RedisCluster.Pipeline do
  use Kino.JS, assets_path: "assets"
  use Kino.SmartCell, name: "RedisCluster: Run Pipeline"

  @impl true
  def init(_attrs, _ctx) do
    fields = %{
      config_variable: "config",
      key: "foo",
      commands: ~s{[["SET", "key", "value"], ["GET", "key"]]}
    }

    {:ok, fields, fields}
  end

  @impl true
  def to_source(%{config_variable: config_var, key: key, commands: commands}) do
    code = """
    # Run pipeline of Redis commands
    commands = #{commands}

    #{config_var} |> RedisCluster.Cluster.pipeline(commands, key: #{inspect(key)})
    """

    {:ok, code}
  end

  @impl true
  def scan_binding(binding, _ctx), do: Map.new(binding)

  @impl true
  def scan_ast(ast, _ctx), do: %{}
end
