defmodule RedisCluster.Cluster.ShardParser do
  @doc """
  Parses the output of the CLUSTER SHARDS command.
  """

  alias RedisCluster.Cluster.NodeInfo

  def parse(data) when is_list(data) do
    Enum.flat_map(data, &parse_node/1)
  end

  defp parse_node(data) do
    %{slots: slots, nodes: nodes} = list_to_map(data)

    for n <- nodes do
      %{id: id, ip: ip, port: port, role: role, health: health} = list_to_map(n)
      role = atomify(role)

      slots =
        slots
        |> Enum.chunk_every(2)
        |> Enum.map(fn [start, stop] ->
          RedisCluster.HashSlots.slot_id(start, stop, role, ip, port)
        end)

      %NodeInfo{
        id: id,
        slots: slots,
        host: ip,
        port: port,
        role: role,
        health: NodeInfo.health_atom(health)
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
