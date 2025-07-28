defmodule RedisCluster.Connection do
  @moduledoc """
  A shim module to start a Redix connection.
  This is necessary to send the [READONLY command](https://redis.io/docs/latest/commands/readonly/) to replicas.
  When in read-write mode, replicas will redirect to the master.
  This must be done per connection, not node.
  """

  alias RedisCluster.Configuration

  @doc false
  @spec start_link(
          {role :: :master | :replica, config :: Configuration.t(), conn_opts :: Keyword.t()}
        ) :: {:ok, pid()}
  def start_link({role, config, conn_opts}) do
    {:ok, pid} = config.redis_module.start_link(conn_opts)

    _ =
      if role == :replica do
        config.redis_module.command!(pid, ["READONLY"])
      end

    {:ok, pid}
  end

  @doc false
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]}
    }
  end
end
