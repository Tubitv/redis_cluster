defmodule RedisCluster.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  # !!!: I probably don't need this module.

  @impl true
  def start(_type, _args) do
    children = [
      # Starts a worker by calling: RedisCluster.Worker.start_link(arg)
      # {RedisCluster.Worker, arg}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: RedisCluster.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
