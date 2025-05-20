defmodule RedisCluster.ClusterInfo do
  @moduledoc """
  A module for fetching the cluster information from a Redis cluster.
  Mostly intended for testing and debugging use. 
  Be aware this module will open a connection for each call.
  """

  alias RedisCluster.Cluster.NodeInfo
  alias RedisCluster.Configuration

  @spec query(Configuration.t()) :: [NodeInfo.t()] | no_return()
  def query(config) do
    {:ok, conn} = Redix.start_link(host: config.host, port: config.port)

    result = fetch_cluster_info(conn, config)

    # Close the connection
    Process.exit(conn, :normal)

    result
  end

  @doc """
  Fetches the cluster information from the Redis cluster and passes it to the provided function.
  If the function returns true, the cluster will be queried again.
  This continues until the function returns false.
  The function also receives the current attempt number (starting from 1).
  The function must handle delays for the next query.
  """
  def query_while(config, fun) when is_function(fun, 2) do
    {:ok, conn} = Redix.start_link(host: config.host, port: config.port)
    query_while(conn, config, 1, fun)
  end

  defp query_while(conn, config, attempt, fun) do
    info = fetch_cluster_info(conn, config)

    if fun.(info, attempt) do
      query_while(conn, config, attempt + 1, fun)
    else
      # Close the connection
      Process.exit(conn, :normal)
      info
    end
  end

  ## Helpers

  defp fetch_cluster_info(conn, config) do
    cluster_shards(conn, config) || cluster_slots(conn, config) || []
  end

  defp cluster_shards(conn, config) do
    case Redix.command(conn, ~w[CLUSTER SHARDS]) do
      {:ok, data} ->
        RedisCluster.Cluster.ShardParser.parse(data)

      {:error, %Redix.Error{message: "ERR This instance has cluster support disabled"}} ->
        standalone_info(conn, config)

      {:error, _} ->
        nil
    end
  end

  defp cluster_slots(conn, config) do
    case Redix.command(conn, ~w[CLUSTER SLOTS]) do
      {:ok, data} ->
        RedisCluster.Cluster.SlotParser.parse(data)

      {:error, %Redix.Error{message: "ERR This instance has cluster support disabled"}} ->
        standalone_info(conn, config)

      {:error, _} ->
        nil
    end
  end

  defp standalone_info(conn, config) do
    case Redix.command(conn, ~w[ROLE]) do
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
    conn = Redix.start_link(host: master.host, port: master.port)
    result = standalone_info(conn, config)

    Process.exit(conn, :normal)

    result
  end
end
