defmodule Nebulex.Adapters.DynamicPartitionedTest do
  use Nebulex.NodeCase

  import Nebulex.CacheCase
  import Nebulex.Helpers

  alias Nebulex.Adapter
  alias Nebulex.TestCache.{DynamicPartitioned, DynamicPartitionedMock}

  @primary :"primary@127.0.0.1"
  @cache_name :dynamic_partitioned_cache

  # Set config
  :ok = Application.put_env(:nebulex, DynamicPartitioned, primary: [backend: :shards])

  test "node leaves and data is still accessable" do
    cache_name = :tony_cache

    node_pid_list =
      start_caches(

        [node() | Node.list()] |> IO.inspect(),
        [
          {DynamicPartitioned, [name: cache_name, join_timeout: 2000]}
          # {DynamicPartitionedMock, []}
        ]
      )

    default_dynamic_cache = DynamicPartitioned.get_dynamic_cache()
    _ = DynamicPartitioned.put_dynamic_cache(cache_name)

    on_exit(fn ->
      IO.puts "closing down"
      _ = DynamicPartitioned.put_dynamic_cache(default_dynamic_cache)
      :ok = Process.sleep(100)
      stop_caches(node_pid_list)
    end)


    alias Nebulex.NodeCase
    alias Nebulex.TestCache.Partitioned

    keys = Enum.map(1..100, fn id ->
      "foo_#{id}"
    end)

    Enum.each(keys, fn key ->
      assert DynamicPartitioned.put("key_#{key}", "value_#{key}") == :ok
    end)

    node_pid = node_pid_list
    |> Enum.reverse()
    |> hd()
    |> elem(2)

    Process.monitor(node_pid)

    :peer.stop(node_pid)

    assert_receive({:DOWN, _ref, :process, _pid, :normal})

    Enum.each(keys, fn key ->
      assert DynamicPartitioned.get("key_#{key}") == "value_#{key}"
    end)
  end
end
