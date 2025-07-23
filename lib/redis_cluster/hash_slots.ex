defmodule RedisCluster.HashSlots do
  @moduledoc """
  A module for tracking the hash slots in a Redis cluster with an ETS table.
  This is not a module you should be calling directly.
  You might use `all_slots/1` or `all_slots_as_table/1` for debugging purposes.
  """

  alias RedisCluster.Configuration
  alias RedisCluster.Lock
  alias RedisCluster.Table

  @ets_key __MODULE__

  @typedoc "The range of hash slot values in a Redis cluster."
  @type hash_slot() :: 0..16_383

  @typedoc "A tuple containing the hash slot info to be stored in ETS."
  @type slot_id() ::
          {__MODULE__, start :: hash_slot(), stop :: hash_slot(), :master | :replica,
           host :: String.t(), port :: non_neg_integer()}

  @doc """
  Creates a named ETS table for tracking hash slots.
  """
  @spec create_table(Configuration.t()) :: :ets.table()
  def create_table(%Configuration{name: name}) do
    Lock.create(name)

    :ets.new(name, [
      :public,
      :duplicate_bag,
      :named_table,
      read_concurrency: true
    ])
  end

  @doc """
  Deletes all entries in the ETS table for the given configuration.
  Doesn't delete the table itself.
  """
  @spec delete(Configuration.t()) :: :ok
  def delete(%Configuration{name: name}) do
    :ets.delete_all_objects(name)
    :ok
  end

  @doc """
  Executes a function with a lock on the ETS table for the given configuration.
  Used to keep operations atomic when modifying the hash slots.
  """
  @spec with_lock(Configuration.t(), (-> term)) :: term
  def with_lock(%Configuration{name: name}, fun) do
    Lock.with_lock(name, fun)
  end

  @doc """
  Convenience function to create a slot ID tuple ready to insert into the ETS table.
  """
  @spec slot_id(
          hash_slot(),
          hash_slot(),
          :master | :replica,
          host :: String.t(),
          port :: non_neg_integer()
        ) ::
          slot_id()
  def slot_id(start, stop, role, host, port) do
    {@ets_key, start, stop, role, host, port}
  end

  @doc """
  Adds a list of slot IDs to the ETS table for the given configuration.
  """
  @spec add_slots(Configuration.t(), [slot_id()]) :: :ok
  def add_slots(%Configuration{name: name}, slots) do
    for id = {@ets_key, _start, _stop, _role, _host, _port} <- slots do
      :ets.insert(name, id)
    end

    :ok
  end

  @doc """
  Returns all slot IDs from the ETS table for the given configuration.
  """
  @spec all_slots(Configuration.t()) :: [slot_id()]
  def all_slots(%Configuration{name: name}) do
    :ets.tab2list(name)
  end

  @doc """
  Formats the results of `all_slots/1` into a human-readable table format.
  Useful to see the current state of the hash slots at a glance.
  This is for debugging purposes only.
  Will not include nodes that were offline or otherwise unhealthy when the cluster was last queried.
  """
  @spec all_slots_as_table(Configuration.t()) :: String.t()
  def all_slots_as_table(config) do
    rows =
      for {@ets_key, start, stop, role, host, port} <- all_slots(config) do
        [start, stop, host, port, role]
      end

    rows
    |> Enum.sort()
    |> Table.rows_to_string(["Slot Start", "Slot End", "Host", "Port", "Role"])
  end

  @doc """
  Looks up the slot ID for a given hash slot and role in the ETS table.
  """
  @spec lookup(Configuration.t(), hash_slot(), :master | :replica | :any, Keyword.t()) ::
          slot_id()
  def lookup(%Configuration{name: name}, hash_slot, role, opts \\ []) do
    retries = Keyword.get(opts, :retries, 10)

    Lock.check_with_retry(name, retries, fn
      :ok ->
        :ets.select(name, match_spec(hash_slot, role))

      _ ->
        []
    end)
  end

  @doc """
  Similar to `lookup/4`, but returns a list of tuples with host and port instead of the full slot ID.
  """
  @spec lookup_conn_info(Configuration.t(), hash_slot(), :master | :replica | :any, Keyword.t()) ::
          [{host :: String.t(), port :: non_neg_integer()}]
  def lookup_conn_info(config, hash_slot, role, opts \\ []) do
    for {@ets_key, _start, _stop, _role, host, port} <- lookup(config, hash_slot, role, opts) do
      {host, port}
    end
  end

  ## Helpers

  # Convenience function to format the appropriate match spec for querying ETS.
  defp match_spec(hash_slot, role) when role in [:master, :replica] do
    [
      {
        {@ets_key, :"$1", :"$2", role, :"$4", :"$5"},
        [
          {:andalso, {:>=, hash_slot, :"$1"}, {:"=<", hash_slot, :"$2"}}
        ],
        [:"$_"]
      }
    ]
  end

  defp match_spec(hash_slot, :any) do
    [
      {
        {@ets_key, :"$1", :"$2", :"$3", :"$4", :"$5"},
        [
          {:andalso, {:>=, hash_slot, :"$1"}, {:"=<", hash_slot, :"$2"}}
        ],
        [:"$_"]
      }
    ]
  end
end
