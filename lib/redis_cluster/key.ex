defmodule RedisCluster.Key do
  @doc """
  Computes the hash slot for the given key. If the `:compute_hash_tag` option is given, 
  then looks for a hash tag in the key and uses it, if present, to compute the hash slot.
  """
  def hash_slot(key, opts \\ []) when is_binary(key) do
    range = Keyword.get(opts, :range, 16_384)

    key
    |> hashable_key(opts)
    |> hash(range)
  end

  @doc """
  Computes the [hash tag](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/#hash-tags)
  of the key, if any.

  # TODO: Add doc tests
  """
  def hashtag(key) do
    with [_leader, rest] <- String.split(key, "{", parts: 2),
         [hashtag, _trailer] when hashtag != "" <- String.split(rest, "}", parts: 2) do
      hashtag
    else
      _ -> nil
    end
  end

  defp hashable_key(key, opts) do
    compute? = Keyword.get(opts, :compute_hash_tag, false)

    if compute? do
      hashtag(key) || key
    else
      key
    end
  end

  defp hash(key, range) do
    :crc_16_xmodem
    |> CRC.crc(key)
    |> rem(range)
  end
end
