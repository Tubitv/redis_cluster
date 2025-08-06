defmodule RedisCluster.MixProject do
  use Mix.Project

  @homepage_url "https://tubitv.hexdocs.pm/redis_cluster"
  @source_url "https://github.com/Tubitv/redis_cluster"
  @version "0.6.1"

  def project do
    [
      app: :redis_cluster,
      version: @version,
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      dialyzer: dialyzer_opts(),
      package: package(),
      docs: docs(),
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp dialyzer_opts do
    [
      plt_add_deps: :app_tree,
      plt_add_apps: [:eex, :mix, :ex_unit],
      ignore_warnings: "dialyzer_ignore.exs",
      flags: [
        # Default warnings
        :unknown,
        :unmatched_returns,
        :error_handling,
        :no_opaque,
        # Extra warnings
        :underspecs
        # Unused warnings
        # :race_conditions,
        # :overspecs
        # :specdiffs
        # Unused suppressions
        # :no_return,
        # :no_fail_call,
        # :no_contracts,
        # :no_missing_calls,
        # :no_match,
        # :no_fun_app,
        # :no_behaviours,
        # :no_improper_lists,
        # :no_undefined_callbacks,
        # :no_unused,
      ]
    ]
  end

  defp deps do
    [
      {:redix, "~> 1.5"},
      {:crc, "~> 0.10"},
      {:telemetry, "~> 1.0"},
      {:ex_doc, "~> 0.34", only: [:dev], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:mox, "~> 1.0", only: [:test]},
      {:kino, "~> 0.16.1"}
    ]
  end

  def package do
    %{
      organization: "tubitv",
      description: "Extends Redix with Redis cluster support",
      files: ~w(.formatter.exs mix.exs lib),
      licenses: "",
      links: %{"GitHub" => @source_url}
    }
  end

  defp docs do
    [
      extras: [],
      main: "RedisCluster",
      source_url: @source_url,
      homepage_url: @homepage_url,
      source_ref: @version,
      formatters: ["html"]
    ]
  end
end
