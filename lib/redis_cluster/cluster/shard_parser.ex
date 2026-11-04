defmodule RedisCluster.Cluster.ShardParser do
  @moduledoc """
  Parses the output of the [CLUSTER SHARDS command](https://redis.io/docs/latest/commands/cluster-shards/).
  This is the expected way to discover a cluster as of Redis v7.0.0.
  Older versions use the [CLUSTER SLOTS command](https://redis.io/docs/latest/commands/cluster-slots/).
  """

  alias RedisCluster.Cluster.NodeInfo

  @doc """
  Parses the output of the CLUSTER SHARDS command.

  Options:
    * `:ssl` - Whether the client connects with TLS (default `false`). When true the
      `tls-port` of each node is preferred over `port`.

  Some managed services (ElastiCache Serverless) report a loopback `ip` (`127.0.0.1`)
  and only a `tls-port`, with the real DNS name in `endpoint`/`hostname`. A loopback `ip`
  is replaced by a non-loopback `endpoint` or `hostname` when one is given.
  """
  def parse(data, opts \\ []) when is_list(data) do
    ssl? = Keyword.get(opts, :ssl, false)
    Enum.flat_map(data, &parse_node(&1, ssl?))
  end

  ## Helpers

  defp parse_node(data, ssl?) do
    %{slots: slots, nodes: nodes} = list_to_map(data)

    for n <- nodes do
      node = list_to_map(n)
      %{id: id, role: role, health: health} = node
      role = atomify(role)
      ip = node_host(node)
      port = node_port(node, ssl?)

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

  defp node_host(node) do
    ip = Map.get(node, :ip)

    if loopback?(ip) do
      Enum.find([Map.get(node, :endpoint), Map.get(node, :hostname)], ip, fn name ->
        is_binary(name) and name != "" and not loopback?(name)
      end)
    else
      ip
    end
  end

  defp loopback?(host), do: host in [nil, "", "127.0.0.1", "::1", "localhost"]

  defp node_port(node, ssl?) do
    plain = Map.get(node, :port)
    tls = Map.get(node, :"tls-port")

    cond do
      ssl? and is_integer(tls) and tls > 0 -> tls
      is_integer(plain) and plain > 0 -> plain
      is_integer(tls) and tls > 0 -> tls
      true -> plain
    end
  end

  defp list_to_map(list) do
    list
    |> Enum.chunk_every(2)
    |> Map.new(fn [k, v] -> {atomify(k), v} end)
  end

  @safe_atomss ~w[slots nodes id port tls-port ip endpoint hostname role replication-offset health master replica]

  defp atomify(key) when key in @safe_atomss do
    String.to_atom(key)
  end

  defp atomify(other) do
    other
  end
end
