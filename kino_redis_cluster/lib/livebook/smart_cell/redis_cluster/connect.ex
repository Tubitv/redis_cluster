defmodule Livebook.SmartCell.RedisCluster.Connect do
  @moduledoc """
  A Livebook smart cell for connecting to Redis Cluster.

  Provides a form-based interface to configure and establish a connection
  to a Redis cluster, returning a `RedisCluster.Configuration` struct.
  """

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

      code
    else
      {:error, reason} ->
        "# Error: #{reason}"
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
          <div class="header">
            <div class="header-content">
              <span class="header-title">RedisCluster</span>
              <span class="header-label">ASSIGN TO</span>
              <input class="header-input" type="text" name="variable" value="${payload.variable || ''}" placeholder="config">
            </div>
          </div>

          <div class="body">
            <div class="field">
              <label class="label">Cluster Name</label>
              <input class="input" type="text" name="name" value="${payload.name || ''}" placeholder="MyRedisCluster">
            </div>

            <div class="field">
              <label class="label">Hostname</label>
              <input class="input" type="text" name="host" value="${payload.host || ''}" placeholder="localhost">
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
                </div>
              </div>
            </div>
          </div>
        </form>
      `;

      ctx.root.innerHTML = formHtml;

      // Add event listeners for form changes
      const form = ctx.root.querySelector("form");
      const inputs = form.querySelectorAll("input");

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
    /* Database cell header styling */
    .header {
      background: #e9eeff;
      border: 1px solid #c7d2fe;
      border-radius: 0.5rem 0.5rem 0 0;
      padding: 0.875rem 1.25rem;
      margin-bottom: 0;
    }

    .header-content {
      display: flex;
      align-items: center;
      gap: 1rem;
    }

    .header-title {
      font-family: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      font-weight: 500;
      font-size: 0.875rem;
      color: #445668;
      margin-right: 0.75rem;
    }

    .header-label {
      font-family: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      font-weight: 500;
      font-size: 0.875rem;
      color: #445668;
      text-transform: uppercase;
      white-space: nowrap;
    }

    .header-input {
      flex: 1;
      max-width: 200px;
      padding: 0.5rem 0.75rem;
      border: 1px solid #c7d2fe;
      border-radius: 0.375rem;
      font-size: 0.875rem;
      background-color: #ffffff;
      color: #374151;
      transition: border-color 0.15s ease-in-out;
    }

    .header-input:focus {
      outline: none;
      border-color: #3b82f6;
    }

    .header-input::placeholder {
      color: #9ca3af;
    }

    /* Body styling */
    .body {
      border: 1px solid #e5e7eb;
      border-top: none;
      border-radius: 0 0 0.5rem 0.5rem;
      padding: 1.25rem;
      background-color: #ffffff;
    }

    .field {
      margin-bottom: 1rem;
    }

    .field:last-child {
      margin-bottom: 0;
    }

    .label {
      display: block;
      font-weight: 500;
      font-size: 0.875rem;
      margin-bottom: 0.375rem;
      color: #6b7280;
      text-transform: none;
    }

    .input {
      width: 100%;
      max-width: 100%;
      box-sizing: border-box;
      padding: 0.625rem 0.75rem;
      border: 1px solid #d1d5db;
      border-radius: 0.375rem;
      font-size: 0.875rem;
      background-color: #f9fafb;
      color: #374151;
      transition: border-color 0.15s ease-in-out, background-color 0.15s ease-in-out;
    }

    .input:hover {
      background-color: #f3f4f6;
    }

    .input:focus {
      outline: none;
      border-color: #3b82f6;
      background-color: #ffffff;
    }

    .input::placeholder {
      color: #9ca3af;
    }

    .help {
      font-size: 0.75rem;
      margin-top: 0.25rem;
      color: #9ca3af;
      line-height: 1.3;
    }

    .columns {
      display: flex;
      gap: 1.5rem;
      align-items: start;
    }

    .column {
      flex: 1;
      min-width: 0;
    }

    .columns .input {
      padding: 0.75rem 1rem;
      box-sizing: border-box;
    }

    /* Dark theme support */
    [data-theme="dark"] .header,
    @media (prefers-color-scheme: dark) {
      .header {
        background: #1e293b;
        border-color: #475569;
      }

      .header-title {
        color: #cbd5e1;
      }

      .header-label {
        color: #cbd5e1;
      }

      .header-input {
        background-color: #1f2937;
        border-color: #4b5563;
        color: #f9fafb;
      }

      .header-input:focus {
        border-color: #60a5fa;
      }

      .header-input::placeholder {
        color: #6b7280;
      }

      .body {
        background-color: #1f2937;
        border-color: #374151;
      }

      .label {
        color: #9ca3af;
      }

      .input {
        background-color: #374151;
        border-color: #4b5563;
        color: #f9fafb;
      }

      .input:hover {
        background-color: #4b5563;
      }

      .input:focus {
        border-color: #60a5fa;
        background-color: #1f2937;
      }

      .input::placeholder {
        color: #6b7280;
      }

      .help {
        color: #6b7280;
      }
    }

    /* Responsive design */
    @media (max-width: 640px) {
      .header-content {
        flex-direction: column;
        align-items: flex-start;
        gap: 0.75rem;
      }

      .header-input {
        max-width: none;
      }

      .columns {
        flex-direction: column;
        gap: 1rem;
      }
    }
    """
  end
end
