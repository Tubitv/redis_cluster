defmodule RedisCluster.Monitor.Message do
  @moduledoc """
  A module for parsing Redis monitor messages.
  """

  require Logger

  @type t() :: %__MODULE__{
          timestamp: float(),
          command: String.t(),
          host: String.t(),
          port: integer()
        }

  @enforce_keys [:timestamp, :command, :host, :port]
  defstruct [:timestamp, :command, :host, :port]

  @doc """
  Parses a Redis monitor message and returns a `RedisCluster.Monitor.Message` struct.

  Example message format:

  ```
  +1753982630.550143 [0 127.0.0.1:53194] "GET" "key"
  ```

      iex> RedisCluster.Monitor.Message.parse(~s(+1753982630.550143 [0 127.0.0.1:53194] "GET" "key"), "127.0.0.1", 6379)
      %RedisCluster.Monitor.Message{
        timestamp: 1753982630.550143,
        command: ~s("GET" "key"),
        host: "127.0.0.1",
        port: 6379
      }
  """
  @spec parse(message :: String.t(), host :: String.t(), port :: integer()) :: t() | nil
  def parse("+OK" <> _, _host, _port) do
    # Immediately ignore any OK responses
    nil
  end

  def parse("+" <> message, host, port) do
    with [timestamp, rest] <- String.split(message, " [", parts: 2, trim: true),
         [_discard, command] <- String.split(rest, "] ", parts: 2, trim: true),
         {timestamp, ""} <- Float.parse(timestamp) do
      %__MODULE__{timestamp: timestamp, command: command, host: host, port: port}
    else
      _ ->
        Logger.warning("Failed to parse Redis monitor message",
          message: message,
          host: host,
          port: port
        )

        nil
    end
  end

  def parse(message, host, port) do
    Logger.warning("Failed to parse Redis monitor message",
      message: message,
      host: host,
      port: port
    )

    nil
  end
end
