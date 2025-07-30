defmodule RedisCluster.RedisAdapter do
  @moduledoc """
  A behaviour module for Redis adapters. This is intended for mocking Redis in the tests.
  Though it could also be used to work with Redis libraries other than Redix.
  """

  @callback start_link(opts :: Keyword.t()) :: {:ok, pid()}
  @callback command(conn :: pid(), cmd :: [binary()]) :: {:ok, binary()}
  @callback pipeline(conn :: pid(), cmds :: [[binary()]]) :: {:ok, [binary()]}
  @callback noreply_pipeline(conn :: pid(), cmds :: [[binary()]]) :: :ok
end
