defmodule RedisCluster.HashSlots do
  alias RedisCluster.Configuration
  alias RedisCluster.Lock

  @ets_key __MODULE__

  @type hash_slot() :: 0..16_383

  @type slot_id() ::
          {__MODULE__, start :: hash_slot(), stop :: hash_slot(), :master | :replica,
           host :: String.t(), port :: non_neg_integer()}

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

  @spec delete(Configuration.t()) :: :ok
  def delete(%Configuration{name: name}) do
    :ets.delete_all_objects(name)
    :ok
  end

  @spec with_lock(Configuration.t(), (() -> term)) :: term
  def with_lock(%Configuration{name: name}, fun) do
    Lock.with_lock(name, fun)
  end

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

  @spec add_slots(Configuration.t(), [slot_id()]) :: :ok
  def add_slots(%Configuration{name: name}, slots) do
    for id = {@ets_key, _start, _stop, _role, _host, _port} <- slots do
      :ets.insert(name, id)
    end

    :ok
  end

  @spec all_slots(Configuration.t()) :: [slot_id()]
  def all_slots(%Configuration{name: name}) do
    :ets.tab2list(name)
  end

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

  @spec lookup_conn_info(Configuration.t(), hash_slot(), :master | :replica | :any, Keyword.t()) ::
          [{host :: String.t(), port :: non_neg_integer()}]
  def lookup_conn_info(config, hash_slot, role, opts \\ []) do
    for {@ets_key, _start, _stop, _role, host, port} <- lookup(config, hash_slot, role, opts) do
      {host, port}
    end
  end

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
