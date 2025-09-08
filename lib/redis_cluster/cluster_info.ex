defmodule RedisCluster.ClusterInfo do
  @moduledoc """
  A module for fetching the cluster information from a Redis cluster.
  Aside from internal use, this may be used for testing and debugging.
  Be aware this module will open a connection for each call.
  """

  alias RedisCluster.Cluster.NodeInfo
  alias RedisCluster.Configuration

  @doc """
  Opens a new connection to the Redis cluster and fetches the cluster information.

  The host and port are taken from the provided configuration.
  Ideally the host should be a configuration endpoint for ElastiCache (or equivalent).
  The connection is closed after the information is fetched.
  """
  @spec query(Configuration.t()) :: [NodeInfo.t()]
  def query(config) do
    options = [host: config.host, port: config.port]

    case config.redis_module.start_link(options) do
      {:ok, conn} ->
        try do
          fetch_cluster_info(conn, config)
        after
          # Close the connection
          GenServer.stop(conn)
        end

      _ ->
        []
    end
  end

  @doc """
  Fetches the cluster information from the Redis cluster and passes it to the provided function.

  If the function returns true, the cluster will be queried again.
  This continues until the function returns false.
  The function also receives the current attempt number (starting from 1).
  The function must handle delays for the next query.
  The advantage of `query_while/2` over `query/1` is that it allows for the connection to be reused.
  The connection is closed when the given function returns false.
  """
  def query_while(config, fun) when is_function(fun, 2) do
    {:ok, conn} = config.redis_module.start_link(host: config.host, port: config.port)
    query_while(conn, config, 1, fun)
  end

  defp query_while(conn, config, attempt, fun) do
    info = fetch_cluster_info(conn, config)

    if fun.(info, attempt) do
      query_while(conn, config, attempt + 1, fun)
    else
      # Close the connection
      GenServer.stop(conn)
      info
    end
  end

  ## Helpers

  defp fetch_cluster_info(conn, config) do
    cluster_shards(conn, config) || cluster_slots(conn, config) || []
  end

  defp cluster_shards(conn, config) do
    case config.redis_module.command(conn, ~w[CLUSTER SHARDS]) do
      {:ok, data} ->
        RedisCluster.Cluster.ShardParser.parse(data)

      {:error, %Redix.Error{message: "ERR This instance has cluster support disabled"}} ->
        standalone_info(conn, config)

      {:error, _} ->
        nil
    end
  end

  defp cluster_slots(conn, config) do
    case config.redis_module.command(conn, ~w[CLUSTER SLOTS]) do
      {:ok, data} ->
        RedisCluster.Cluster.SlotParser.parse(data)

      {:error, %Redix.Error{message: "ERR This instance has cluster support disabled"}} ->
        standalone_info(conn, config)

      {:error, _} ->
        nil
    end
  end

  defp standalone_info(conn, config) do
    case config.redis_module.command(conn, ~w[ROLE]) do
      {:ok, data} ->
        data
        |> RedisCluster.Cluster.RoleParser.parse(config)
        |> fetch_remaining_replicas(config)

      {:error, _} ->
        nil
    end
  end

  defp fetch_remaining_replicas({:full, master, replicas}, _config) do
    [master | replicas]
  end

  defp fetch_remaining_replicas({:partial, master, _replicas}, config) do
    conn = config.redis_module.start_link(host: master.host, port: master.port)
    result = standalone_info(conn, config)

    GenServer.stop(conn)

    result
  end
end
