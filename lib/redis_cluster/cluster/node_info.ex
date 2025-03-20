defmodule RedisCluster.Cluster.NodeInfo do
  @doc """
  Struct holding info from the CLUSTER SHARDS or CLUSTER SLOTS commands.
  """

  @type t :: %__MODULE__{
          id: String.t(),
          slots: [RedisCluster.HashSlots.t()],
          host: String.t(),
          port: non_neg_integer(),
          role: :master | :replica
        }

  @enforce_keys [:id, :slots, :host, :port, :role]
  defstruct [:id, :slots, :host, :port, :role]
end
