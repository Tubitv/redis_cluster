defmodule SimpleConnectCell do
  use Kino.JS
  use Kino.JS.Live
  use Kino.SmartCell, name: "Simple Redis Connect"

  @impl true
  def init(attrs, ctx) do
    variable = attrs["variable"] || "config"
    name = attrs["name"] || "MyRedisCluster"
    host = attrs["host"] || "localhost"
    port = attrs["port"] || "6379"
    pool_size = attrs["pool_size"] || "10"

    {:ok, assign(ctx, variable: variable, name: name, host: host, port: port, pool_size: pool_size)}
  end

  @impl true
  def handle_connect(ctx) do
    payload = %{
      variable: ctx.assigns.variable,
      name: ctx.assigns.name,
      host: ctx.assigns.host,
      port: ctx.assigns.port,
      pool_size: ctx.assigns.pool_size
    }
    {:ok, payload, ctx}
  end

    @impl true
  def handle_event("update", params, ctx) do
    ctx = assign(ctx, params)
    {:noreply, ctx}
  end

          @impl true
  def to_attrs(ctx) do
    # Use string keys (from events) with atom key fallbacks (from init)
    %{
      "variable" => ctx.assigns["variable"] || ctx.assigns.variable,
      "name" => ctx.assigns["name"] || ctx.assigns.name,
      "host" => ctx.assigns["host"] || ctx.assigns.host,
      "port" => ctx.assigns["port"] || ctx.assigns.port,
      "pool_size" => ctx.assigns["pool_size"] || ctx.assigns.pool_size
    }
  end

  @impl true
  def scan_binding(binding, _env, _ctx) do
    # Return any existing connection variables that might be referenced
    binding
    |> Enum.filter(fn {key, value} ->
      # Only include variables that are atoms/strings and contain redis/cluster
      # Skip PIDs and other non-enumerable values
      is_atom(key) and is_struct(value) and
        (String.contains?(to_string(key), "redis") or String.contains?(to_string(key), "cluster"))
    end)
    |> Map.new()
  rescue
    _ -> %{}
  end

                                      @impl true
  def to_source(attrs) do
    # Use attrs values with fallbacks
    variable = attrs["variable"] || "config"
    name = attrs["name"] || "MyRedisCluster"
    host = attrs["host"] || "localhost"
    port = attrs["port"] || "6379"
    pool_size = attrs["pool_size"] || "10"

    # Basic validation
    port_num = case Integer.parse(port) do
      {num, ""} when num > 0 and num <= 65535 -> num
      _ -> 6379
    end

    pool_num = case Integer.parse(pool_size) do
      {num, ""} when num > 0 and num <= 100 -> num
      _ -> 10
    end

                        # Use the specified variable name, clean it up for safety
    var_name = variable |> String.trim() |> String.replace(~r/[^a-zA-Z0-9_]/, "_")

    """
# Redis Cluster Connection: #{name}
name = #{inspect(name)}

#{var_name} = %RedisCluster.Configuration{
  name: String.to_atom(name),
  host: #{inspect(host)},
  port: #{port_num},
  pool_size: #{pool_num},
  registry: Module.concat([name, Registry]),
  pool: Module.concat([name, Pool]),
  cluster: Module.concat([name, Cluster]),
  shard_discovery: Module.concat([name, ShardDiscovery])
}

# Start the cluster only if not already started
case RedisCluster.Cluster.start_link(#{var_name}) do
  {:ok, _pid} -> :ok
  {:error, {:already_started, _pid}} -> :ok
  {:error, reason} -> raise "Failed to start Redis cluster: \#{inspect(reason)}"
end

# Configuration is now available as: #{var_name}
#{var_name}
"""
  end

  asset "main.js" do
    """
    export function init(ctx, payload) {
      console.log("SimpleConnectCell init:", payload);

      ctx.root.innerHTML = `
        <div style="padding: 1rem; border: 1px solid #ccc; border-radius: 4px;">
          <h4>Redis Cluster Connection</h4>
          <div style="margin-bottom: 1rem;">
            <label>Assign to:</label><br>
            <input type="text" name="variable" value="${payload.variable || ''}" placeholder="config" style="width: 100%; padding: 4px;">
            <small style="color: #666;">Variable name to store the connection</small>
          </div>
          <div style="margin-bottom: 1rem;">
            <label>Cluster Name:</label><br>
            <input type="text" name="name" value="${payload.name || ''}" style="width: 100%; padding: 4px;">
          </div>
          <div style="margin-bottom: 1rem;">
            <label>Host:</label><br>
            <input type="text" name="host" value="${payload.host || ''}" style="width: 100%; padding: 4px;">
          </div>
          <div style="margin-bottom: 1rem;">
            <label>Port:</label><br>
            <input type="number" name="port" value="${payload.port || ''}" style="width: 100%; padding: 4px;">
          </div>
          <div style="margin-bottom: 1rem;">
            <label>Pool Size:</label><br>
            <input type="number" name="pool_size" value="${payload.pool_size || ''}" style="width: 100%; padding: 4px;">
          </div>
        </div>
      `;

      const inputs = ctx.root.querySelectorAll("input");

            // Add event listeners to form inputs
      inputs.forEach(input => {
        input.addEventListener("change", () => {
          const formData = {};
          inputs.forEach(inp => {
            if (inp.name) {
              formData[inp.name] = inp.value;
            }
          });
          ctx.pushEvent("update", formData);
        });
      });

      // Send form data on sync
      ctx.handleSync(() => {
        const formData = {};
        inputs.forEach(inp => {
          if (inp.name) {
            formData[inp.name] = inp.value;
          }
        });
        ctx.pushEvent("update", formData);
      });
    }
    """
  end
end
