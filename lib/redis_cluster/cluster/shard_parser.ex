defmodule RedisCluster.Cluster.ShardParser do
  @doc """
  Parses the output of the CLUSTER SHARDS command.
  """

  def parse(data) when is_list(data) do
    Enum.flat_map(data, &parse_node/1)
  end

  defp parse_node(data) do
    %{slots: slots, nodes: nodes} = list_to_map(data)

    for n <- nodes do
      map = list_to_map(n)

      slots =
        slots
        |> Enum.chunk_every(2)
        |> Enum.map(&RedisCluster.HashSlots.slot_id/1)

      %RedisCluster.Cluster.NodeInfo{
        id: map.id,
        slots: slots,
        host: map.ip,
        port: map.port,
        role: atomify(map.role)
      }
    end
  end

  defp list_to_map(list) do
    list
    |> Enum.chunk_every(2)
    |> Map.new(fn [k, v] -> {atomify(k), v} end)
  end

  @safe_atomss ~w[slots nodes id port ip endpoint role replication-offset health master replica]

  defp atomify(key) when key in @safe_atomss do
    String.to_atom(key)
  end

  defp atomify(other) do
    other
  end
end
