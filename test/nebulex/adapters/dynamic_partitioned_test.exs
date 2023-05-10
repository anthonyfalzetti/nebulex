defmodule Nebulex.Adapters.DynamicPartitionedTest do
  use Nebulex.NodeCase
  use Nebulex.CacheTest

  import Nebulex.CacheCase
  import Nebulex.Helpers

  alias Nebulex.Adapter
  alias Nebulex.TestCache.{DynamicPartitioned, DynamicPartitionedMock}

  @primary :"primary@127.0.0.1"
  @cache_name :dynamic_partitioned_cache

  # Set config
  :ok = Application.put_env(:nebulex, DynamicPartitioned, primary: [backend: :shards])

  setup do
    cluster = :lists.usort([@primary | Application.get_env(:nebulex, :nodes, [])])

    node_pid_list =
      start_caches(
        [node() | Node.list()],
        [
          {DynamicPartitioned, [name: @cache_name, join_timeout: 2000]},
          {DynamicPartitionedMock, []}
        ]
      )

    default_dynamic_cache = DynamicPartitioned.get_dynamic_cache()
    _ = DynamicPartitioned.put_dynamic_cache(@cache_name)

    on_exit(fn ->
      _ = DynamicPartitioned.put_dynamic_cache(default_dynamic_cache)
      :ok = Process.sleep(100)
      stop_caches(node_pid_list)
    end)

    {:ok, cache: DynamicPartitioned, name: @cache_name, cluster: cluster}
  end

  describe "c:init/1" do
    test "initializes the primary store metadata" do
      Adapter.with_meta(DynamicPartitionedCache.Primary, fn adapter, meta ->
        assert adapter == Nebulex.Adapters.Local
        assert meta.backend == :shards
      end)
    end

    test "raises an exception because invalid primary store" do
      assert_raise ArgumentError, ~r"adapter Invalid was not compiled", fn ->
        defmodule Demo do
          use Nebulex.Cache,
            otp_app: :nebulex,
            adapter: Nebulex.Adapters.DynamicPartitioned,
            primary_storage_adapter: Invalid
        end
      end
    end

    test "fails because unloaded keyslot module" do
      assert {:error, {%ArgumentError{message: msg}, _}} =
               DynamicPartitioned.start_link(
                 name: :unloaded_keyslot,
                 keyslot: UnloadedKeyslot
               )

      assert Regex.match?(~r"keyslot UnloadedKeyslot was not compiled", msg)
    end

    test "fails because keyslot module does not implement expected behaviour" do
      assert {:error, {%ArgumentError{message: msg}, _}} =
               DynamicPartitioned.start_link(
                 name: :invalid_keyslot,
                 keyslot: __MODULE__
               )

      mod = inspect(__MODULE__)
      behaviour = "Nebulex.Adapter.Keyslot"
      assert Regex.match?(~r"expected #{mod} to implement the behaviour #{behaviour}", msg)
    end

    test "fails because invalid keyslot option" do
      assert {:error, {%ArgumentError{message: msg}, _}} =
               DynamicPartitioned.start_link(
                 name: :invalid_keyslot,
                 keyslot: "invalid"
               )

      assert Regex.match?(~r"expected keyslot: to be an atom, got: \"invalid\"", msg)
    end
  end

  describe "partitioned cache" do
    test "custom keyslot" do
      defmodule Keyslot do
        @behaviour Nebulex.Adapter.Keyslot

        @impl true
        def hash_slot(key, range) do
          key
          |> :erlang.phash2()
          |> rem(range)
        end
      end

      test_with_dynamic_cache(DynamicPartitioned, [name: :custom_keyslot, keyslot: Keyslot], fn ->
        refute DynamicPartitioned.get("foo")
        assert DynamicPartitioned.put("foo", "bar") == :ok
        assert DynamicPartitioned.get("foo") == "bar"
      end)
    end

    test "get_and_update" do
      assert DynamicPartitioned.get_and_update(1, &DynamicPartitioned.get_and_update_fun/1) ==
               {nil, 1}

      assert DynamicPartitioned.get_and_update(1, &DynamicPartitioned.get_and_update_fun/1) ==
               {1, 2}

      assert DynamicPartitioned.get_and_update(1, &DynamicPartitioned.get_and_update_fun/1) ==
               {2, 4}

      assert_raise ArgumentError, fn ->
        DynamicPartitioned.get_and_update(1, &DynamicPartitioned.get_and_update_bad_fun/1)
      end
    end

    test "incr raises when the counter is not an integer" do
      :ok = DynamicPartitioned.put(:counter, "string")

      assert_raise ArgumentError, fn ->
        DynamicPartitioned.incr(:counter, 10)
      end
    end
  end

  describe "cluster scenario:" do
    test "node leaves and then rejoins", %{name: name, cluster: cluster} do
      assert node() == @primary
      assert :lists.usort(Node.list()) == cluster -- [node()]
      assert DynamicPartitioned.nodes() == cluster

      DynamicPartitioned.with_dynamic_cache(name, fn ->
        :ok = DynamicPartitioned.leave_cluster()
        assert DynamicPartitioned.nodes() == cluster -- [node()]
      end)

      DynamicPartitioned.with_dynamic_cache(name, fn ->
        :ok = DynamicPartitioned.join_cluster()
        assert DynamicPartitioned.nodes() == cluster
      end)
    end

    test "teardown cache node", %{cluster: cluster} do
      assert DynamicPartitioned.nodes() == cluster

      assert DynamicPartitioned.put(1, 1) == :ok
      assert DynamicPartitioned.get(1) == 1

      node = teardown_cache(1)

      wait_until(fn ->
        assert DynamicPartitioned.nodes() == cluster -- [node]
      end)

      refute DynamicPartitioned.get(1)

      assert :ok == DynamicPartitioned.put_all([{4, 44}, {2, 2}, {1, 1}])

      assert DynamicPartitioned.get(4) == 44
      assert DynamicPartitioned.get(2) == 2
      assert DynamicPartitioned.get(1) == 1
    end

    test "bootstrap leaves cache from the cluster when terminated and then rejoins when restarted",
         %{name: name} do
      prefix = [:nebulex, :test_cache, :dynamic_partitioned, :bootstrap]
      started = prefix ++ [:started]
      stopped = prefix ++ [:stopped]
      joined = prefix ++ [:joined]
      exit_sig = prefix ++ [:exit]

      with_telemetry_handler(__MODULE__, [started, stopped, joined, exit_sig], fn ->
        assert node() in DynamicPartitioned.nodes()

        true =
          [name, Bootstrap]
          |> normalize_module_name()
          |> Process.whereis()
          |> Process.exit(:stop)

        assert_receive {^exit_sig, %{system_time: _}, %{reason: :stop}}, 5000
        assert_receive {^stopped, %{system_time: _}, %{reason: :stop, cluster_nodes: nodes}}, 5000

        refute node() in nodes

        assert_receive {^started, %{system_time: _}, %{}}, 5000
        assert_receive {^joined, %{system_time: _}, %{cluster_nodes: nodes}}, 5000

        assert node() in nodes
        assert nodes -- DynamicPartitioned.nodes() == []

        :ok = Process.sleep(2100)

        assert_receive {^joined, %{system_time: _}, %{cluster_nodes: nodes}}, 5000
        assert node() in nodes
      end)
    end
  end

  describe "rpc" do
    test "timeout error" do
      assert DynamicPartitioned.put_all(for(x <- 1..100_000, do: {x, x}), timeout: 60_000) == :ok
      assert DynamicPartitioned.get(1, timeout: 1000) == 1

      msg = ~r"RPC error while executing action :all\n\nSuccessful responses:"

      assert_raise Nebulex.RPCMultiCallError, msg, fn ->
        DynamicPartitioned.all(nil, timeout: 1)
      end
    end

    test "runtime error" do
      _ = Process.flag(:trap_exit, true)

      assert [1, 2] |> DynamicPartitionedMock.get_all(timeout: 10) |> map_size() == 0

      assert DynamicPartitionedMock.put_all(a: 1, b: 2) == :ok

      assert [1, 2] |> DynamicPartitionedMock.get_all() |> map_size() == 0

      assert_raise ArgumentError, fn ->
        DynamicPartitionedMock.get(1)
      end

      msg = ~r"RPC error while executing action :count_all\n\nSuccessful responses:"

      assert_raise Nebulex.RPCMultiCallError, msg, fn ->
        DynamicPartitionedMock.count_all()
      end
    end
  end

  if Code.ensure_loaded?(:erpc) do
    describe ":erpc" do
      test "timeout error" do
        assert DynamicPartitioned.put(1, 1) == :ok
        assert DynamicPartitioned.get(1, timeout: 1000) == 1

        node = "#{inspect(DynamicPartitioned.get_node(1))}"
        reason = "#{inspect({:erpc, :timeout})}"

        msg = ~r"The RPC operation failed on node #{node} with reason:\n\n#{reason}"

        assert_raise Nebulex.RPCError, msg, fn ->
          DynamicPartitioned.get(1, timeout: 0)
        end
      end
    end
  end

  ## Private Functions

  defp teardown_cache(key) do
    node = DynamicPartitioned.get_node(key)
    remote_pid = :rpc.call(node, Process, :whereis, [@cache_name])
    :ok = :rpc.call(node, Supervisor, :stop, [remote_pid])
    node
  end
end
