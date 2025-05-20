defmodule RedisCluster.Cluster.RoleParser do
  @moduledoc """
  Parses the output of the ROLE command.
  """

  @max_slot_number 16_383

  alias RedisCluster.Cluster.NodeInfo

  def parse(["master", _offset, replicas], config) do
    replicas =
      Enum.map(replicas, fn [host, port, _offset] ->
        port = String.to_integer(port)

        %NodeInfo{
          id: "",
          slots: [
            RedisCluster.HashSlots.slot_id(0, @max_slot_number, :replica, host, port)
          ],
          host: host,
          port: port,
          role: :replica,
          health: :online
        }
      end)

    master = %NodeInfo{
      id: "",
      slots: [
        RedisCluster.HashSlots.slot_id(0, @max_slot_number, :master, config.host, config.port)
      ],
      host: config.host,
      port: config.port,
      role: :master,
      health: :online
    }

    {:full, master, replicas}
  end

  def parse([role, master_host, master_port, _health, _offset], config)
      when role in ~w[replica slave] do
    replica = %NodeInfo{
      id: "",
      slots: [
        RedisCluster.HashSlots.slot_id(0, @max_slot_number, :replica, config.host, config.port)
      ],
      host: config.host,
      port: config.port,
      role: :replica,
      health: :online
    }

    master = %NodeInfo{
      id: "",
      slots: [
        RedisCluster.HashSlots.slot_id(0, @max_slot_number, :master, config.host, config.port)
      ],
      host: master_host,
      port: String.to_integer(master_port),
      role: :master,
      health: :online
    }

    # We can't know all the replicas from a replica. 
    # Need to query the master for that.
    {:partial, master, [replica]}
  end

  def parse(["sentinel", _masters], _config) do
    raise "Sentinel role not supported"
  end
end
