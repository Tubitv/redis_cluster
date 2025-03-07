defmodule RedisCluster.Cluster.NodeInfo do
  @doc """
  Struct holding info from the CLUSTER SHARDS or CLUSTER SLOTS commands.
  """

  @enforce_keys [:id, :slots, :host, :port, :role]
  defstruct [:id, :slots, :host, :port, :role]
end
