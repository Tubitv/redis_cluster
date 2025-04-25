defmodule RedisCluster.Cluster.NodeInfo do
  @doc """
  Struct holding info from the CLUSTER SHARDS or CLUSTER SLOTS commands.
  """

  @type t :: %__MODULE__{
          id: String.t(),
          slots: [RedisCluster.HashSlots.hash_slot()],
          host: String.t(),
          port: non_neg_integer(),
          role: :master | :replica
        }

  @enforce_keys [:id, :slots, :host, :port, :role]
  defstruct [:id, :slots, :host, :port, :role]

  @doc """
  Converts the node info into a table format for display.
  May take a single node or a list of nodes.
  Returns `iodata` that can be printed directly or converted to a string.
  """
  def to_table(info) do
    rows =
      info
      |> List.wrap()
      |> Enum.flat_map(fn node ->
        for {_, start, stop, role, host, port} <- node.slots do
          {start, stop, host, port, role}
        end
      end)
      |> Enum.sort()

    headers = ["Slot Start", "Slot End", "Host", "Port", "Role"]

    RedisCluster.Table.rows_to_iodata(rows, headers)
  end
end
