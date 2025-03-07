defmodule RedisCluster.MixProject do
  use Mix.Project

  def project do
    [
      app: :redis_cluster,
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {RedisCluster.Application, []}
    ]
  end

  defp deps do
    [
      {:redix, "~> 1.5"},
      {:crc, "~> 0.10"},
      # Just for debugging in Livebook
      {:kino, "~> 0.15.3"}
    ]
  end
end
