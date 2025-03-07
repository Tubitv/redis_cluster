defmodule RedisCluster.Connection do
  @doc """
  A shim module to start a Redix connection.
  This is necessary to send the READONLY command to replicas.
  When in read-write mode, replicas will redirect to the master.
  This must be done per connection, not node.
  """

  @doc false
  def start_link({role, conn_opts}) do
    {:ok, pid} = Redix.start_link(conn_opts)

    if role == :replica do
      Redix.command!(pid, ["READONLY"])
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
