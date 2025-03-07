defmodule RedisCluster.HashSlots do
  alias RedisCluster.Lock

  @ets_key :redis_cluster_slot

  def create_table(name) do
    Lock.create(name)

    :ets.new(name, [
      :public,
      :duplicate_bag,
      :named_table,
      read_concurrency: true
    ])
  end

  def delete(name) do
    :ets.delete_all_objects(name)
  end

  def with_lock(name, fun) do
    Lock.with_lock(name, fun)
  end

  def slot_id([start, stop]) do
    slot_id(start, stop)
  end

  def slot_id(start, stop) do
    {@ets_key, start, stop}
  end

  def add_slots(name, slots, opts \\ []) do
    for id = {@ets_key, _start, _stop} <- slots do
      :ets.insert(name, id)
    end
  end

  def lookup(name, hash_slot, opts \\ []) do
    retries = Keyword.get(opts, :retries, 10)

    Lock.check_with_retry(name, retries, fn
      :ok ->
        match_spec = [
          {
            {@ets_key, :"$1", :"$2"},
            [
              {:andalso, {:>=, hash_slot, :"$1"}, {:"=<", hash_slot, :"$2"}}
            ],
            [:"$_"]
          }
        ]

        case :ets.select(name, match_spec) do
          [value | _] -> value
          _ -> nil
        end

      _ ->
        nil
    end)
  end
end
