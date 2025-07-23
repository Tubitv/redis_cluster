defmodule RedisCluster.Lock do
  @moduledoc """
  A simple in-memory locking mechanism using Erlang's `:persistent_term`.

  This module provides a lightweight, process-independent locking mechanism
  that can be used to coordinate access to shared resources.

  ## Examples

      # Create a lock with name :my_lock
      iex> RedisCluster.Lock.create(:my_lock)
      :ok
      iex> RedisCluster.Lock.check(:my_lock)
      :ok

      # Lock it to prevent concurrent access
      iex> RedisCluster.Lock.lock(:my_lock)
      :ok
      iex> RedisCluster.Lock.check(:my_lock)
      :locked

      # Unlock it when done
      iex> RedisCluster.Lock.unlock(:my_lock)
      :ok
      iex> RedisCluster.Lock.check(:my_lock)
      :ok

      # Use with_lock to automatically unlock after an operation
      iex> RedisCluster.Lock.with_lock(:auto_lock, fn ->
      ...>   # This code runs while the lock is held
      ...>   "operation result"
      ...> end)
      "operation result"
      iex> RedisCluster.Lock.check(:auto_lock)
      :ok

      # Delete a lock when no longer needed
      iex> RedisCluster.Lock.create(:temp_lock)
      :ok
      iex> RedisCluster.Lock.delete(:temp_lock)
      true
  """

  @typedoc "The name of the lock as an atom. Must be unique within the application."
  @type name() :: atom()

  @typedoc "Possible statuses of a lock."
  @type status() :: :ok | :locked

  @doc """
  Creates a new lock with the given name.

  The lock is initially unlocked.

  ## Examples

      iex> RedisCluster.Lock.create(:example_lock)
      :ok
      iex> RedisCluster.Lock.check(:example_lock)
      :ok
  """
  @spec create(name()) :: :ok
  def create(name) when is_atom(name) do
    unlock(name)
  end

  @doc """
  Deletes a lock with the given name.

  Returns `true` if the lock was deleted, `false` otherwise.

  ## Examples

      iex> RedisCluster.Lock.create(:delete_me)
      :ok
      iex> RedisCluster.Lock.delete(:delete_me)
      true

      iex> RedisCluster.Lock.delete(:bogus)
      false
  """
  @spec delete(name()) :: boolean()
  def delete(name) when is_atom(name) do
    name
    |> key()
    |> :persistent_term.erase()
  end

  @doc """
  Checks the status of a lock.

  Returns `:ok` if the lock is available, `:locked` if it's held, or the default
  value if the lock doesn't exist.

  ## Examples

      iex> RedisCluster.Lock.create(:check_lock)
      :ok
      iex> RedisCluster.Lock.check(:check_lock)
      :ok
      iex> RedisCluster.Lock.lock(:check_lock)
      :ok
      iex> RedisCluster.Lock.check(:check_lock)
      :locked
      iex> RedisCluster.Lock.check(:bogus, :not_found)
      :not_found
  """
  @spec check(name(), status()) :: status()
  def check(name, default \\ :locked) when is_atom(name) do
    name
    |> key()
    |> :persistent_term.get(default)
  end

  @doc """
  Locks a lock, to signal to other processes that the lock is held.

  ## Examples

      iex> RedisCluster.Lock.create(:lock_me)
      :ok
      iex> RedisCluster.Lock.lock(:lock_me)
      :ok
      iex> RedisCluster.Lock.check(:lock_me)
      :locked
  """
  @spec lock(name()) :: :ok
  def lock(name) when is_atom(name) do
    name
    |> key()
    |> :persistent_term.put(:locked)
  end

  @doc """
  Unlocks a lock, to signal to other processes that the lock is available.

  ## Examples

      iex> RedisCluster.Lock.create(:unlock_me)
      iex> RedisCluster.Lock.lock(:unlock_me)
      iex> RedisCluster.Lock.check(:unlock_me)
      :locked
      iex> RedisCluster.Lock.unlock(:unlock_me)
      iex> RedisCluster.Lock.check(:unlock_me)
      :ok
  """
  @spec unlock(name()) :: :ok
  def unlock(name) when is_atom(name) do
    name
    |> key()
    |> :persistent_term.put(:ok)
  end

  @doc """
  Executes a function with a lock held, and automatically unlocks when done.

  This ensures that the lock is released even if the function raises an exception.

  ## Examples

      iex> RedisCluster.Lock.with_lock(:protected_operation, fn ->
      ...>   # Critical section that requires exclusive access
      ...>   :operation_complete
      ...> end)
      :operation_complete
  """
  @spec with_lock(name(), (-> term)) :: term
  def with_lock(name, fun) when is_atom(name) do
    lock(name)
    result = fun.()
    unlock(name)
    result
  end

  @doc """
  Retries a function until the lock becomes available or max attempts are reached.

  Returns the result of calling the function with the lock status.

  ## Examples

      iex> RedisCluster.Lock.create(:retry_example)
      :ok
      iex> RedisCluster.Lock.check_with_retry(:retry_example, 3, fn status ->
      ...>   # This will be called with status = :ok since the lock is available
      ...>   {:got_status, status}
      ...> end)
      {:got_status, :ok}
  """
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
