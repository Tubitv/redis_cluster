defmodule RedisCluster.Lock do
  @type name() :: atom()
  @type status() :: :ok | :locked

  @spec create(name()) :: :ok
  def create(name) when is_atom(name) do
    unlock(name)
  end

  @spec delete(name()) :: :ok
  def delete(name) when is_atom(name) do
    name
    |> key()
    |> :persistent_term.erase()
  end

  @spec check(name(), status()) :: status()
  def check(name, default \\ :locked) when is_atom(name) do
    name
    |> key()
    |> :persistent_term.get(default)
  end

  @spec lock(name()) :: :ok
  def lock(name) when is_atom(name) do
    name
    |> key()
    |> :persistent_term.put(:locked)
  end

  @spec unlock(name()) :: :ok
  def unlock(name) when is_atom(name) do
    name
    |> key()
    |> :persistent_term.put(:ok)

    :ok
  end

  @spec with_lock(name(), (() -> term)) :: term
  def with_lock(name, fun) when is_atom(name) do
    lock(name)
    result = fun.()
    unlock(name)
    result
  end

  @spec check_with_retry(name(), pos_integer(), (status() -> term)) :: term
  def check_with_retry(name, retries, fun) when is_atom(name) do
    check_with_retry(name, 1, max(1, retries - 1), fun)
  end

  defp check_with_retry(name, attempts, max_attempts, fun)
       when max_attempts == :infinity
       when is_integer(max_attempts) and attempts <= max_attempts do
    case check(name) do
      :ok ->
        fun.(:ok)

      :locked ->
        attempts |> backoff_ms() |> Process.sleep()
        check_with_retry(name, attempts + 1, max_attempts, fun)
    end
  end

  defp check_with_retry(_name, _attempts, _max_attempts, fun) do
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
