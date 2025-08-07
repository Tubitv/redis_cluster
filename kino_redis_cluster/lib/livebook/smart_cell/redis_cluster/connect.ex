defmodule Livebook.SmartCell.RedisCluster.Connect do
  use Kino.JS
  use Kino.JS.Live
  use Kino.SmartCell, name: "RedisCluster: Connect"

  @impl true
  def init(attrs, ctx) do
    variable = attrs["variable"] || "config"
    name = attrs["name"] || "MyRedisCluster"
    host = attrs["host"] || "localhost"
    port = attrs["port"] || "6379"
    pool_size = attrs["pool_size"] || "10"

    ctx = assign(ctx, variable: variable, name: name, host: host, port: port, pool_size: pool_size)
    {:ok, ctx}
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
    |> Enum.filter(fn {key, _value} ->
      String.contains?(to_string(key), "redis") or String.contains?(to_string(key), "cluster")
    end)
    |> Map.new()
  end

  @impl true
  def to_source(attrs) do
    variable = attrs["variable"] || "config"
    name = attrs["name"] || "MyRedisCluster"
    host = attrs["host"] || "localhost"
    port = attrs["port"] || "6379"
    pool_size = attrs["pool_size"] || "10"

    # Validate required fields
    with {:ok, validated_name} <- validate_name(name),
         {:ok, validated_host} <- validate_host(host),
         {:ok, validated_port} <- validate_port(port),
         {:ok, validated_pool_size} <- validate_pool_size(pool_size) do

      # Use the specified variable name, clean it up for safety
      var_name = variable |> String.trim() |> String.replace(~r/[^a-zA-Z0-9_]/, "_")

      code = """
      # Redis Cluster Connection: #{validated_name}
      name = #{inspect(validated_name)}

      #{var_name} = %RedisCluster.Configuration{
        name: String.to_atom(name),
        host: #{inspect(validated_host)},
        port: #{validated_port},
        pool_size: #{validated_pool_size},
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

      {:ok, code}
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  # Private helper functions

  defp validate_name(name) when is_binary(name) do
    trimmed = String.trim(name)
    if trimmed != "" do
      {:ok, trimmed}
    else
      {:error, "Cluster name cannot be empty"}
    end
  end

  defp validate_host(host) when is_binary(host) do
    trimmed = String.trim(host)
    if trimmed != "" do
      {:ok, trimmed}
    else
      {:error, "Host cannot be empty"}
    end
  end

  defp validate_port(port) when is_binary(port) do
    case Integer.parse(port) do
      {port_num, ""} when port_num > 0 and port_num <= 65535 ->
        {:ok, port_num}
      _ ->
        {:error, "Port must be a number between 1 and 65535"}
    end
  end

  defp validate_pool_size(pool_size) when is_binary(pool_size) do
    case Integer.parse(pool_size) do
      {size, ""} when size > 0 and size <= 100 ->
        {:ok, size}
      _ ->
        {:error, "Pool size must be a number between 1 and 100"}
    end
  end

  asset "main.js" do
    """
    export function init(ctx, payload) {
      ctx.importCSS("main.css");

      // Create form elements
      const formHtml = `
        <form>
          <div class="field">
            <label class="label">Assign to</label>
            <input class="input" type="text" name="variable" value="${payload.variable || ''}" placeholder="config">
            <p class="help">Variable name to store the connection</p>
          </div>

          <div class="field">
            <label class="label">Cluster Name</label>
            <input class="input" type="text" name="name" value="${payload.name || ''}" placeholder="MyRedisCluster">
            <p class="help">A unique name for this Redis cluster connection</p>
          </div>

          <div class="field">
            <label class="label">Host</label>
            <input class="input" type="text" name="host" value="${payload.host || ''}" placeholder="localhost">
            <p class="help">Redis cluster node hostname or IP address</p>
          </div>

          <div class="columns">
            <div class="column">
              <div class="field">
                <label class="label">Port</label>
                <input class="input" type="number" name="port" value="${payload.port || ''}" placeholder="6379" min="1" max="65535">
              </div>
            </div>
            <div class="column">
              <div class="field">
                <label class="label">Pool Size</label>
                <input class="input" type="number" name="pool_size" value="${payload.pool_size || ''}" placeholder="10" min="1" max="100">
                <p class="help">Number of connections per node</p>
              </div>
            </div>
          </div>
        </form>
      `;

      ctx.root.innerHTML = formHtml;

      // Add event listeners for form changes
      const form = ctx.root.querySelector("form");
      const inputs = form.querySelectorAll("input");

      inputs.forEach(input => {
        input.addEventListener("input", (event) => {
          const field = event.target.name;
          const value = event.target.value;

          // Send update to server
          const updates = {};
          updates[field] = value;
          ctx.pushEvent("update", updates);
        });
      });

      // Handle server updates
      ctx.handleEvent("update", (payload) => {
        Object.entries(payload).forEach(([field, value]) => {
          const input = form.querySelector(`[name="${field}"]`);
          // Only update if the input is not currently focused (being edited by user)
          if (input && input.value !== value && document.activeElement !== input) {
            input.value = value;
          }
        });
      });

      // Handle synchronization - send all form data
      ctx.handleSync(() => {
        const formData = {};
        const inputs = form.querySelectorAll("input");
        inputs.forEach(input => {
          if (input.name) {
            formData[input.name] = input.value;
          }
        });
        ctx.pushEvent("update", formData);
      });
    }
    """
  end

  asset "main.css" do
    """
    .field {
      margin-bottom: 1rem;
    }

    .label {
      display: block;
      font-weight: 600;
      margin-bottom: 0.5rem;
      color: #374151;
    }

    .input {
      width: 100%;
      padding: 0.5rem 0.75rem;
      border: 1px solid #d1d5db;
      border-radius: 0.375rem;
      font-size: 0.875rem;
      background-color: #ffffff;
      color: #374151;
      transition: border-color 0.15s ease-in-out, box-shadow 0.15s ease-in-out;
    }

    .input:focus {
      outline: none;
      border-color: #3b82f6;
      box-shadow: 0 0 0 0.125rem rgba(59, 130, 246, 0.25);
    }

    .help {
      font-size: 0.75rem;
      margin-top: 0.25rem;
      color: #6b7280;
    }

    .columns {
      display: flex;
      gap: 1rem;
    }

    .column {
      flex: 1;
    }

    /* Dark theme support */
    @media (prefers-color-scheme: dark) {
      .label {
        color: #f3f4f6;
      }

      .input {
        background-color: #1f2937;
        border-color: #4b5563;
        color: #f3f4f6;
      }

      .input:focus {
        border-color: #60a5fa;
      }

      .help {
        color: #9ca3af;
      }
    }
    """
  end
end
