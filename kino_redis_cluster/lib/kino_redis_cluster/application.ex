defmodule KinoRedisCluster.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    Kino.SmartCell.register(Livebook.SmartCell.RedisCluster.Connect)
    Kino.SmartCell.register(Livebook.SmartCell.RedisCluster.Pipeline)

    children = [
      # Starts a worker by calling: KinoRedisCluster.Worker.start_link(arg)
      # {KinoRedisCluster.Worker, arg}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: KinoRedisCluster.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
