defmodule KinoRedisCluster.MixProject do
  use Mix.Project

  @homepage_url "https://hexdocs.pm/kino_redis_cluster"
  @source_url "https://github.com/Tubitv/redis_cluster"
  @version "0.1.1"

  def project do
    [
      app: :kino_redis_cluster,
      version: @version,
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      package: package(),
      docs: docs(),
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {KinoRedisCluster.Application, []}
    ]
  end

  defp deps do
    [
      {:redis_cluster, "~> 0.8"},
      {:kino, "~> 0.16"},
      {:jason, "~> 1.4"},
      {:ex_doc, "~> 0.34", only: [:dev], runtime: false}
    ]
  end

  def package do
    %{
      description: "Livebook extension for RedisCluster",
      files: ~w(.formatter.exs mix.exs lib priv),
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url}
    }
  end

  defp docs do
    [
      extras: ["README.md"],
      main: "readme",
      source_url: @source_url,
      homepage_url: @homepage_url,
      source_ref: "v#{@version}",
      formatters: ["html"]
    ]
  end
end
