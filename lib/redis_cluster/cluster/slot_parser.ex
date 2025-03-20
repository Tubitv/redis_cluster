defmodule RedisCluster.Cluster.SlotParser do
  @doc """
  Parses the output of the CLUSTER SLOTS command.
  """

  def parse(data) when is_list(data) do
    data
    |> Enum.flat_map(&parse_node/1)
    |> consolidate_slots()
  end

  defp parse_node([start, stop | nodes]) do
    for {[host, port, id | _meta], n} <- Enum.with_index(nodes) do
      role = if(n == 0, do: :master, else: :replica)

      %RedisCluster.Cluster.NodeInfo{
        id: id,
        slots: [RedisCluster.HashSlots.slot_id(start, stop, role, host, port)],
        host: host,
        port: port,
        role: role
      }
    end
  end

  defp consolidate_slots(slots) do
    slots
    |> Enum.group_by(& &1.id)
    |> Enum.map(fn {_, nodes} ->
      Enum.reduce(nodes, fn node, acc ->
        %RedisCluster.Cluster.NodeInfo{node | slots: Enum.sort(acc.slots ++ node.slots)}
      end)
    end)
  end
end
