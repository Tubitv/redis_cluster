defmodule Livebook.SmartCell.RedisCluster.Pipeline do
  @moduledoc """
  A Livebook smart cell for executing Redis pipeline commands on a cluster.

  Provides a dynamic form interface to build and execute multiple Redis commands
  in a single pipeline operation, with support for string interpolation and
  quoted arguments.
  """

  use Kino.JS
  use Kino.JS.Live
  use Kino.SmartCell, name: "RedisCluster: Run Pipeline"

  @impl true
  def init(attrs, ctx) do
    variable = attrs["variable"] || "result"
    config_variable = attrs["config_variable"] || "config"
    key = attrs["key"] || "key"
    role = attrs["role"] || "master"
    # Parse commands from JSON or create default
    commands = parse_commands_from_attrs(attrs["commands"]) || ["GET key"]

    ctx = assign(ctx, variable: variable, config_variable: config_variable, key: key, role: role, commands: commands)
    {:ok, ctx}
  end

  @impl true
  def handle_connect(ctx) do
    payload = %{
      variable: ctx.assigns.variable,
      config_variable: ctx.assigns.config_variable,
      key: ctx.assigns.key,
      role: ctx.assigns.role,
      commands: ctx.assigns.commands
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
      "config_variable" => ctx.assigns["config_variable"] || ctx.assigns.config_variable,
      "key" => ctx.assigns["key"] || ctx.assigns.key,
      "role" => ctx.assigns["role"] || ctx.assigns.role,
      "commands" => ctx.assigns["commands"] || ctx.assigns.commands
    }
  end

  @impl true
  def to_source(attrs) do
    variable = attrs["variable"] || "result"
    config_var = attrs["config_variable"] || "config"
    key = attrs["key"] || "key"
    role = attrs["role"] || "master"
    commands = attrs["commands"] || ["GET key"]

    # Parse commands from list of strings
    parsed_commands = parse_command_strings(commands)

    # Determine key parameter based on input
    key_param = case String.trim(key) do
      "any" -> ":any"
      ":any" -> ":any"
      other_key ->
        "\"" <> other_key <> "\""
    end

    # Determine role parameter
    role_param = case String.trim(role) do
      "any" -> ":any"
      "master" -> ":master"
      "replica" -> ":replica"
      _ -> ":master"
    end

    code = """
    # Redis Cluster Pipeline Commands
    commands = #{format_commands_for_output(parsed_commands)}

    #{variable} = RedisCluster.Cluster.pipeline(#{config_var}, commands, #{key_param}, [role: #{role_param}])
    """

    code
  end

  @impl true
  def scan_binding(_binding, _env, _ctx) do
    # The pipeline cell doesn't need to scan for existing variables
    # It creates a new result variable
    %{}
  end

  # Private helper functions

  defp parse_commands_from_attrs(commands_json) when is_binary(commands_json) do
    case Jason.decode(commands_json) do
      {:ok, commands} when is_list(commands) ->
        Enum.map(commands, fn
          cmd when is_list(cmd) -> Enum.join(cmd, " ")
          cmd when is_binary(cmd) -> cmd
          _ -> "GET key"
        end)
      _ -> nil
    end
  end
  defp parse_commands_from_attrs(_), do: nil

  defp parse_command_strings(command_strings) do
    command_strings
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.map(&parse_command_with_quotes/1)
  end

  defp parse_command_with_quotes(command_string) do
    # Parse command string respecting quoted values
    # Handles both single and double quotes
    command_string
    |> String.trim()
    |> parse_quoted_string([])
  end

  defp parse_quoted_string("", acc), do: Enum.reverse(acc)

  defp parse_quoted_string(str, acc) do
    str = String.trim_leading(str)

    cond do
      str == "" ->
        Enum.reverse(acc)

      String.starts_with?(str, "\"") ->
        # Handle double quotes
        case extract_quoted_value(str, "\"") do
          {value, rest} -> parse_quoted_string(rest, [value | acc])
          :error -> parse_unquoted_word(str, acc)
        end

      String.starts_with?(str, "'") ->
        # Handle single quotes
        case extract_quoted_value(str, "'") do
          {value, rest} -> parse_quoted_string(rest, [value | acc])
          :error -> parse_unquoted_word(str, acc)
        end

      true ->
        # Handle unquoted word
        parse_unquoted_word(str, acc)
    end
  end

  defp extract_quoted_value(str, quote_char) do
    # Remove opening quote
    str = String.slice(str, 1..-1//-1)

    case String.split(str, quote_char, parts: 2) do
      [value, rest] -> {value, rest}
      [_] -> :error  # No closing quote found
    end
  end

  defp parse_unquoted_word(str, acc) do
    case String.split(str, ~r/\s+/, parts: 2) do
      [word, rest] -> parse_quoted_string(rest, [word | acc])
      [word] -> parse_quoted_string("", [word | acc])
    end
  end

  defp format_commands_for_output(commands) do
    commands
    |> Enum.map(fn cmd ->
      cmd
      |> Enum.map(&format_command_argument/1)
      |> Enum.join(", ")
      |> then(&"[#{&1}]")
    end)
    |> Enum.join(",\n  ")
    |> then(&"[\n  #{&1}\n]")
  end

  defp format_command_argument(arg) do
    "\"" <> arg <> "\""
  end

  asset "main.js" do
    """
    export function init(ctx, payload) {
      ctx.importCSS("main.css");

      let state = {
        variable: payload.variable || 'result',
        config_variable: payload.config_variable || 'config',
        key: payload.key || 'key',
        role: payload.role || 'master',
        commands: payload.commands || ['GET key']
      };

      function render() {
        const formHtml = `
          <form>
            <div class="header">
              <div class="header-content">
                <span class="header-title">RedisCluster</span>
                <span class="header-label">CONFIGURATION</span>
                <input class="header-input" type="text" name="config_variable" value="${state.config_variable}" placeholder="config">
                <span class="header-label">ASSIGN TO</span>
                <input class="header-input" type="text" name="variable" value="${state.variable || ''}" placeholder="result">
                <button type="button" class="help-button" title="Toggle help">
                  <span class="help-icon">?</span>
                </button>
              </div>
              <div class="help-content" style="display: none;">
                <p>You may use Elixir string interpolation in the routing key, like <code>\#{key}</code>.</p>
                <p>For commands that don't need a key, use <code>:any</code> as the key.</p>
                <p>To dynamically inject values into commands use Elixir string interpolation, like <code> SET \#{key} \#{value}</code>.</p>
              </div>
            </div>

            <div class="body">
              <div class="columns">
                <div class="column">
                  <div class="field">
                    <label class="label">Key</label>
                    <input class="input" type="text" name="key" value="${state.key}" placeholder="key">
                  </div>
                </div>
                <div class="column">
                  <div class="field">
                    <label class="label">Role</label>
                    <select class="input" name="role" value="${state.role || 'master'}">
                      <option value="any" ${state.role === 'any' ? 'selected' : ''}>Any</option>
                      <option value="master" ${(state.role || 'master') === 'master' ? 'selected' : ''}>Master</option>
                      <option value="replica" ${state.role === 'replica' ? 'selected' : ''}>Replica</option>
                    </select>
                  </div>
                </div>
              </div>

              <div class="field">
                <label class="label">Pipeline Commands</label>
                <div class="commands-container">
                  ${renderCommands()}
                </div>
                <button type="button" class="button add-command">
                  <span class="icon">+</span>
                  <span>Add Command</span>
                </button>
              </div>
            </div>
          </form>
        `;

        ctx.root.innerHTML = formHtml;
        attachEventListeners();
      }

      function renderCommands() {
        return state.commands.map((cmd, index) => {
          const isFirst = index === 0;
          return `
            <div class="command-row" data-index="${index}">
              <div class="command-input-group">
                <input
                  class="input command-input"
                  type="text"
                  value="${cmd}"
                  placeholder="GET key"
                  data-index="${index}"
                >
                ${!isFirst ? `
                  <button type="button" class="button is-danger is-small remove-command" data-index="${index}">
                    <span class="icon">×</span>
                  </button>
                ` : ''}
              </div>
            </div>
          `;
        }).join('');
      }

      function attachEventListeners() {
        const form = ctx.root.querySelector("form");

        // Handle basic field updates
        form.querySelectorAll('input[name="config_variable"], input[name="key"], input[name="variable"], select[name="role"]').forEach(input => {
          input.addEventListener("change", (event) => {
            state[event.target.name] = event.target.value;
            updateServer();
          });
        });

        // Handle help button toggle
        const helpButton = form.querySelector('.help-button');
        if (helpButton) {
          helpButton.addEventListener('click', (event) => {
            event.preventDefault();
            const helpContent = form.querySelector('.help-content');
            if (helpContent.style.display === 'none') {
              helpContent.style.display = 'block';
            } else {
              helpContent.style.display = 'none';
            }
          });
        }

        // Handle command input changes
        form.querySelectorAll('.command-input').forEach(input => {
          input.addEventListener("change", (event) => {
            const index = parseInt(event.target.dataset.index);
            state.commands[index] = event.target.value;
            updateServer();
          });
        });

        // Handle add command button
        const addButton = form.querySelector('.add-command');
        if (addButton) {
          addButton.addEventListener('click', (event) => {
            event.preventDefault();
            addCommandRow();
          });
        }

        // Handle remove command buttons using event delegation
        form.addEventListener('click', (event) => {
          if (event.target.closest('.remove-command')) {
            event.preventDefault();
            const button = event.target.closest('.remove-command');
            const index = parseInt(button.dataset.index);
            removeCommandRow(index);
          }
        });
      }

      function addCommandRow() {
        const commandsContainer = ctx.root.querySelector('.commands-container');
        const newIndex = state.commands.length;

        // Add to state
        state.commands.push('GET key');

        // Create new DOM element
        const commandRow = document.createElement('div');
        commandRow.className = 'command-row';
        commandRow.dataset.index = newIndex;

        commandRow.innerHTML = `
          <div class="command-input-group">
            <input
              class="input command-input"
              type="text"
              value='GET key'
              placeholder='GET key'
              data-index="${newIndex}"
            >
            <button type="button" class="button is-danger is-small remove-command" data-index="${newIndex}">
              <span class="icon">×</span>
            </button>
          </div>
        `;

        commandsContainer.appendChild(commandRow);

        // Attach event listeners to new elements
        const newInput = commandRow.querySelector('.command-input');
        newInput.addEventListener("change", (event) => {
          const index = parseInt(event.target.dataset.index);
          state.commands[index] = event.target.value;
          updateServer();
        });

        // Remove button event listener is handled by event delegation

        updateServer();
      }

      function removeCommandRow(indexToRemove) {
        // Remove from state
        state.commands.splice(indexToRemove, 1);

        // Remove DOM element
        const rowToRemove = ctx.root.querySelector(`[data-index="${indexToRemove}"]`);
        if (rowToRemove) {
          rowToRemove.remove();
        }

        // Update indices for remaining rows
        const remainingRows = ctx.root.querySelectorAll('.command-row');
        remainingRows.forEach((row, newIndex) => {
          row.dataset.index = newIndex;
          const input = row.querySelector('.command-input');
          const button = row.querySelector('.remove-command');

          if (input) input.dataset.index = newIndex;
          if (button) button.dataset.index = newIndex;

          // Show/hide remove button based on whether it's first
          if (button) {
            button.style.display = newIndex === 0 ? 'none' : 'inline-flex';
          }
        });

        updateServer();
      }

      function updateServer() {
        ctx.pushEvent("update", state);
      }

      // Send form data on sync
      ctx.handleSync(() => {
        updateServer();
      });

      // Initial render
      render();
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
      flex-wrap: wrap;
    }

    .header-title {
      font-family: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      font-weight: 800;
      font-size: 1rem;
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
      margin-left: 0.5rem;
    }

    .header-label:first-of-type {
      margin-left: 0;
    }

    .header-input {
      flex: 0 0 auto;
      width: 120px;
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

    .help-button {
      flex: 0 0 auto;
      width: 24px;
      height: 24px;
      border: 1px solid #c7d2fe;
      border-radius: 50%;
      background-color: #ffffff;
      color: #445668;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: center;
      margin-left: auto;
      transition: all 0.15s ease-in-out;
    }

    .help-button:hover {
      background-color: #f0f4ff;
      border-color: #a5b4fc;
    }

    .help-button:focus {
      outline: none;
      border-color: #3b82f6;
      box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.1);
    }

    .help-icon {
      font-size: 0.75rem;
      font-weight: 600;
      line-height: 1;
    }

    .help-content {
      margin-top: 0.75rem;
      padding: 0.75rem 1rem;
      background-color: #f8fafc;
      border: 1px solid #e2e8f0;
      border-radius: 0.375rem;
      font-size: 0.875rem;
      color: #475569;
      line-height: 1.4;
    }

    .help-content p {
      margin: 0;
    }

    .help-content code {
      background-color: #e2e8f0;
      color: #1e293b;
      padding: 0.125rem 0.25rem;
      border-radius: 0.25rem;
      font-family: ui-monospace, SFMono-Regular, "SF Mono", Consolas, "Liberation Mono", Menlo, monospace;
      font-size: 0.8125rem;
    }

    /* Body styling */
    .body {
      border: 1px solid #e5e7eb;
      border-top: none;
      border-radius: 0 0 0.5rem 0.5rem;
      padding: 1.25rem;
      background-color: #ffffff;
    }

    /* Header dark theme support */
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

      .help-button {
        background-color: #1f2937;
        border-color: #4b5563;
        color: #cbd5e1;
      }

      .help-button:hover {
        background-color: #374151;
        border-color: #6b7280;
      }

      .help-button:focus {
        border-color: #60a5fa;
        box-shadow: 0 0 0 2px rgba(96, 165, 250, 0.1);
      }

      .help-content {
        background-color: #1f2937;
        border-color: #374151;
        color: #cbd5e1;
      }

      .help-content code {
        background-color: #374151;
        color: #f1f5f9;
      }

      .body {
        background-color: #1f2937;
        border-color: #374151;
      }
    }

    /* Modern Livebook styling for Pipeline */
    .field {
      margin-bottom: 1.25rem;
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
      margin-bottom: 1.5rem;
    }

    .column {
      flex: 1;
      min-width: 0;
    }

    /* Command-specific styling */
    .commands-container {
      margin-bottom: 1rem;
      padding: 1rem;
      background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%);
      border: 1.5px solid #e2e8f0;
      border-radius: 0.75rem;
      box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.05);
    }

    .command-row {
      margin-bottom: 0.75rem;
      animation: slideIn 0.2s ease-out;
    }

    .command-row:last-child {
      margin-bottom: 0;
    }

    .command-input-group {
      display: flex;
      gap: 0.75rem;
      align-items: center;
      padding: 0.5rem;
      background-color: #ffffff;
      border: 1px solid #e5e7eb;
      border-radius: 0.5rem;
      box-shadow: 0 1px 2px 0 rgba(0, 0, 0, 0.03);
      transition: all 0.2s ease-in-out;
    }

    .command-input-group:hover {
      border-color: #d1d5db;
      box-shadow: 0 2px 4px 0 rgba(0, 0, 0, 0.06);
    }

    .command-input {
      flex: 1;
      padding: 0.5rem;
      border: none;
      background: transparent;
      font-family: 'SF Mono', 'Monaco', 'Inconsolata', 'Roboto Mono', monospace;
      font-size: 0.875rem;
      color: #374151;
      border-radius: 0.25rem;
    }

    .command-input:focus {
      outline: none;
      background-color: #f8fafc;
    }

    /* Button styling */
    .button {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      padding: 0.625rem 1rem;
      border: 1.5px solid transparent;
      border-radius: 0.5rem;
      font-size: 0.875rem;
      font-weight: 600;
      cursor: pointer;
      transition: all 0.2s ease-in-out;
      text-decoration: none;
      gap: 0.5rem;
      box-shadow: 0 1px 2px 0 rgba(0, 0, 0, 0.05);
    }

    .add-command {
      background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%);
      color: white;
      border-color: #2563eb;
    }

    .add-command:hover {
      background: linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%);
      box-shadow: 0 4px 12px 0 rgba(37, 99, 235, 0.4);
      transform: translateY(-1px);
    }

    .add-command:active {
      transform: translateY(0);
      box-shadow: 0 2px 4px 0 rgba(37, 99, 235, 0.4);
    }

    .remove-command {
      background: linear-gradient(135deg, #ef4444 0%, #dc2626 100%);
      color: white;
      border-color: #dc2626;
      padding: 0.375rem;
      border-radius: 0.375rem;
      font-size: 0.75rem;
      min-width: 2rem;
      height: 2rem;
    }

    .remove-command:hover {
      background: linear-gradient(135deg, #dc2626 0%, #b91c1c 100%);
      box-shadow: 0 4px 12px 0 rgba(220, 38, 38, 0.4);
      transform: translateY(-1px);
    }

    .remove-command:active {
      transform: translateY(0);
      box-shadow: 0 2px 4px 0 rgba(220, 38, 38, 0.4);
    }

    .icon {
      font-weight: bold;
      font-size: 1rem;
      line-height: 1;
    }



    /* Animations */
    @keyframes slideIn {
      from {
        opacity: 0;
        transform: translateY(-10px);
      }
      to {
        opacity: 1;
        transform: translateY(0);
      }
    }

    /* Dark theme support */
    [data-theme="dark"] .label,
    @media (prefers-color-scheme: dark) {
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

      .commands-container {
        background: linear-gradient(135deg, #1f2937 0%, #111827 100%);
        border-color: #374151;
      }

      .command-input-group {
        background-color: #1f2937;
        border-color: #374151;
      }

      .command-input-group:hover {
        border-color: #4b5563;
      }

      .command-input {
        color: #f9fafb;
      }

      .command-input:focus {
        background-color: #111827;
      }

      .add-command {
        background: linear-gradient(135deg, #60a5fa 0%, #3b82f6 100%);
        border-color: #3b82f6;
      }

      .add-command:hover {
        background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%);
        box-shadow: 0 4px 12px 0 rgba(96, 165, 250, 0.4);
      }

      .remove-command {
        background: linear-gradient(135deg, #f87171 0%, #ef4444 100%);
        border-color: #ef4444;
      }

      .remove-command:hover {
        background: linear-gradient(135deg, #ef4444 0%, #dc2626 100%);
        box-shadow: 0 4px 12px 0 rgba(239, 68, 68, 0.4);
      }
    }

    /* Responsive design */
    @media (max-width: 768px) {
      .header-content {
        flex-direction: column;
        align-items: flex-start;
        gap: 0.75rem;
      }

      .header-input {
        width: 100%;
        max-width: 200px;
      }

      .header-label {
        margin-left: 0;
      }

      .columns {
        flex-direction: column;
        gap: 1rem;
      }

      .command-input-group {
        flex-direction: column;
      }

      .button.is-small {
        align-self: flex-start;
      }
    }
    """
  end
end
