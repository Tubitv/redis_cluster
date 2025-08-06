defmodule KinoRedisCluster.MixProject do
  use Mix.Project

  def project do
    [
      app: :kino_redis_cluster,
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
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
      {:redis_cluster, path: ".."},
      {:kino, "~> 0.16"}
    ]
  end
end
