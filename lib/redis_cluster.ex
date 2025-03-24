defmodule RedisCluster do
  @moduledoc """
  This module is a thin wrapper around the `RedisCluster.Cluster` module.

  First, you will need a module to `use` this module.

  ```elixir
  defmodule MyApp.Redis do
    use RedisCluster, otp_app: :my_app
  end
  ```

  Then you need to include the relevant config in your `config.exs` (or `runtime.exs`):

  ```elixir
  config :my_app, MyApp.Redis,
    host: "localhost",
    port: 6379,
    pool_size: 10
  ```

  Ideally, your `host` should a configuration endpoint for ElastiCache (or equivalent).
  This endpoint picks a random node in the cluster to connect to for discovering the cluster.

  Your module will have basic Redis functions like `get`, `set`, and `delete`.
  You can also run arbitrary Redis commands with `command` and `pipeline`.

  You may also want to include other convenience functions in your module:

  ```elixir
  defmodule MyApp.Redis do
    use RedisCluster, otp_app: :my_app

    def hget(key, field) do
      # Implementation here...
    end

    def hgetall(key) do
      # Implementation here...
    end

    def hset(key, field, value) do
      # Implementation here...
    end
  end
  ```

  Alternatively, you can use the `RedisCluster.Cluster` module directly.

  ```elixir
  config = %RedisCluster.Configuration{
    host: "localhost",
    port: 6379,
    pool_size: 3,
    name: Test.Redis,
    registry: Test.Redis.Registry__,
    pool: Test.Redis.Pool__,
    cluster: Test.Redis.Cluster__,
    shard_discovery: Test.Redis.ShardDiscovery__
  }

  {:ok, pid} = RedisCluster.Cluster.start_link(config)

  RedisCluster.Cluster.set(config, "answer", 42)

  RedisCluster.Cluster.get(config, "key")
  ```

  This can be useful for testing, Livebook demos, or dynamically connecting to Redis clusters.
  """

  defmacro __using__(macro_opts) do
    quote bind_quoted: [macro_opts: macro_opts] do
      @config RedisCluster.Configuration.from_app_env(macro_opts, __MODULE__)

      def start_link(_) do
        RedisCluster.Cluster.start_link(@config)
      end

      def config() do
        @config
      end

      def get(key, opts \\ []) do
        RedisCluster.Cluster.get(@config, key, opts)
      end

      def get_many(keys, opts \\ []) do
        RedisCluster.Cluster.get_many(@config, keys, opts)
      end

      def set(key, value, opts \\ []) do
        RedisCluster.Cluster.set(@config, key, value, opts)
      end

      def set_many(entries, opts \\ []) do
        RedisCluster.Cluster.set_many(@config, entries, opts)
      end

      def delete(key, opts \\ []) do
        RedisCluster.Cluster.delete(@config, key, opts)
      end

      def delete_many(keys, opts \\ []) do
        RedisCluster.Cluster.delete_many(@config, keys, opts)
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
