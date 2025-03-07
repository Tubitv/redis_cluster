defmodule RedisCluster.Lock do
  def create(name) when is_atom(name) do
    unlock(name)
  end

  def delete(name) when is_atom(name) do
    name
    |> key()
    |> :persistent_term.erase()
  end

  def check(name, default \\ :locked) when is_atom(name) do
    name
    |> key()
    |> :persistent_term.get(default)
  end

  def lock(name) when is_atom(name) do
    name
    |> key()
    |> :persistent_term.put(:locked)
  end

  def unlock(name) when is_atom(name) do
    name
    |> key()
    |> :persistent_term.put(:ok)
  end

  def with_lock(name, fun) when is_atom(name) do
    lock(name)
    fun.()
    unlock(name)
  end

  def check_with_retry(name, retries, fun) when is_atom(name) do
    check_with_retry(name, 1, retries, fun)
  end

  defp check_with_retry(name, attempts, max_retries, fun)
       when max_retries == :infinity
       when is_integer(max_retries) and attempts <= max_retries do
    case check(name) do
      :ok ->
        fun.(:ok)

      :locked ->
        attempts |> backoff_ms() |> Process.sleep()
        check_with_retry(name, attempts + 1, max_retries, fun)
    end
  end

  defp check_with_retry(_name, _attempts, _max_retries, fun) do
    fun.(:locked)
  end

  # Inline common instructions
  @compile {:inline, key: 1, backoff_ms: 1}

  defp key(name) do
    {name, :redis_cluster_lock_status}
  end

  defp backoff_ms(1), do: 500
  defp backoff_ms(2), do: 1000
  defp backoff_ms(3), do: 2000
  defp backoff_ms(4), do: 4000
  defp backoff_ms(_), do: 8000
end
