defmodule KinoRedisCluster.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

    @impl true
  def start(_type, _args) do
    # Register the Redis Cluster smart cells
    Kino.SmartCell.register(Livebook.SmartCell.RedisCluster.Connect)
    Kino.SmartCell.register(Livebook.SmartCell.RedisCluster.Pipeline)

    children = []

    opts = [strategy: :one_for_one, name: KinoRedisCluster.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
