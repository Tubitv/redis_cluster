defmodule RedisCluster.Key do
  @max_slot 16_384
  @type hash() :: 0..16_383

  @doc """
  Computes the hash slot for the given key. If the `:compute_hash_tag` option is given, 
  then looks for a hash tag in the key and uses it, if present, to compute the hash slot.

  ## Options

    * `:compute_hash_tag` - If set to `true`, the hash tag will be computed from the key.
      This is useful for cluster mode, where keys with the same hash tag are stored
      in the same slot.

  ## Examples

      iex> RedisCluster.Key.hash_slot("my_key")
      13711

      iex> RedisCluster.Key.hash_slot("my_key", compute_hash_tag: true)
      13711

      iex> RedisCluster.Key.hash_slot("{user1234}:contact", compute_hash_tag: true)
      14020

      iex> RedisCluster.Key.hash_slot("{user1234}:search_history", compute_hash_tag: true)
      14020
  """
  @spec hash_slot(binary(), Keyword.t()) :: hash()
  def hash_slot(key, opts \\ []) when is_binary(key) do
    key
    |> hashable_key(opts)
    |> hash()
  end

  @doc """
  Computes the [hash tag](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/#hash-tags)
  of the key, if any.

  ## Examples

      iex> RedisCluster.Key.hashtag("{user1234}:contact")
      "user1234"

      iex> RedisCluster.Key.hashtag("search_history:{user1234}")
      "user1234"

      iex> RedisCluster.Key.hashtag("{user1234}:orders:{not_a_tag}")
      "user1234"

      iex> RedisCluster.Key.hashtag("{}:" <> <<0xDE, 0xAD, 0xC0, 0xDE>>)
      nil

      iex> RedisCluster.Key.hashtag("{}:some_key:{not_a_tag}")
      nil

      iex> RedisCluster.Key.hashtag("my_key")
      nil
  """
  @spec hashtag(binary()) :: binary() | nil
  def hashtag(key) do
    with [_leader, rest] <- String.split(key, "{", parts: 2),
         [hashtag, _trailer] when hashtag != "" <- String.split(rest, "}", parts: 2) do
      hashtag
    else
      _ -> nil
    end
  end

  @spec hashable_key(binary(), Keyword.t()) :: binary()
  defp hashable_key(key, opts) do
    compute? = Keyword.get(opts, :compute_hash_tag, false)

    if compute? do
      hashtag(key) || key
    else
      key
    end
  end

  @spec hash(binary()) :: hash()
  defp hash(key) do
    :crc_16_xmodem
    |> CRC.crc(key)
    |> rem(@max_slot)
  end
end
