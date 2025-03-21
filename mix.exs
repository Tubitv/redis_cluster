defmodule RedisCluster.MixProject do
  use Mix.Project

  def project do
    [
      app: :redis_cluster,
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      dialyzer: dialyzer_opts(),
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
      # Just for debugging in Livebook
      {:kino, "~> 0.15.3"},
      {:dialyxir, "~> 1.4", only: [:dev], runtime: false}
    ]
  end
end
