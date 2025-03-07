defmodule RedisCluster do
  @moduledoc """
  Documentation for `RedisCluster`.
  """

  defmacro __using__(macro_opts) do
    quote bind_quoted: [macro_opts: macro_opts] do
      @config RedisCluster.Config.from_app_env(macro_opts, __MODULE__)

      def start_link(_) do
        RedisCluster.Cluster.start_link(@config)
      end

      def config() do
        @config
      end

      def get(key, opts \\ []) do
        RedisCluster.Cluster.get(@config, key, opts)
      end

      def set(key, value, opts \\ []) do
        RedisCluster.Cluster.set(@config, key, value, opts)
      end

      def command(command, opts \\ []) do
        RedisCluster.Cluster.command(@config, command, opts)
      end

      def command!(command, opts \\ []) do
        command
        |> command(opts)
        |> bang!()
      end

      def pipeline(commands, opts \\ []) do
        RedisCluster.Cluster.pipeline(@config, commands, opts)
      end

      def pipeline!(commands, opts \\ []) do
        commands
        |> pipeline(opts)
        |> bang!()
      end

      defp bang!(result) do
        case result do
          {:ok, response} -> response
          {:error, error} -> raise error
        end
      end
    end
  end
end
