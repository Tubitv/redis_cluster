defmodule Livebook.SmartCell.RedisCluster.Pipeline do
  use Kino.JS
  use Kino.JS.Live
  use Kino.SmartCell, name: "RedisCluster: Run Pipeline"

  @impl true
  def init(attrs, ctx) do
    config_variable = attrs["config_variable"] || "config"
    key = attrs["key"] || "\#{key}"
    # Parse commands from JSON or create default
    commands = parse_commands_from_attrs(attrs["commands"]) || ["GET key"]

    ctx = assign(ctx, config_variable: config_variable, key: key, commands: commands)
    {:ok, ctx}
  end

  @impl true
  def handle_connect(ctx) do
    payload = %{
      config_variable: ctx.assigns.config_variable,
      key: ctx.assigns.key,
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
      "config_variable" => ctx.assigns["config_variable"] || ctx.assigns.config_variable,
      "key" => ctx.assigns["key"] || ctx.assigns.key,
      "commands" => ctx.assigns["commands"] || ctx.assigns.commands
    }
  end

  @impl true
  def to_source(attrs) do
    config_var = attrs["config_variable"] || "config"
    key = attrs["key"] || "\#{key}"
    commands = attrs["commands"] || ["GET key"]

    # Parse commands from list of strings
    parsed_commands = parse_command_strings(commands)

    # Determine key parameter based on input
    key_param = case String.trim(key) do
      "any" -> ":any"
      ":any" -> ":any"
      other_key ->
        # Check if key contains interpolation (#{...})
        if String.contains?(other_key, "\#{") do
          # Return as string with interpolation - don't quote it so it evaluates
          "\"#{other_key}\""
        else
          # Return as regular quoted string
          inspect(other_key)
        end
    end

    code = """
    # Redis Cluster Pipeline Commands
    commands = #{format_commands_for_output(parsed_commands)}

    RedisCluster.Cluster.pipeline(#{config_var}, commands, #{key_param}, [])
    """

    code
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
    # Check if argument contains interpolation
    if String.contains?(arg, "\#{") do
      # Return as string with interpolation - don't escape it
      "\"#{arg}\""
    else
      # Return as regular quoted string
      inspect(arg)
    end
  end

  asset "main.js" do
    """
    export function init(ctx, payload) {
      ctx.importCSS("main.css");

      let state = {
        config_variable: payload.config_variable || 'config',
        key: payload.key || '\#{key}',
        commands: payload.commands || ['GET key']
      };

      function render() {
        const formHtml = `
          <form>
            <div class="field">
              <label class="label">Configuration Variable</label>
              <input class="input" type="text" name="config_variable" value="${state.config_variable}" placeholder="config">
              <p class="help">Variable name containing the RedisCluster.Configuration struct</p>
            </div>

            <div class="field">
              <label class="label">Key</label>
              <input class="input" type="text" name="key" value="${state.key}" placeholder="\#{key}">
              <p class="help">Redis key for hash slot routing (use :any for any node, supports interpolation like \#{key})</p>
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
              <p class="help">Redis commands to execute in pipeline. Space-separated with quoted values (e.g., SET key "some value")</p>
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

      function getCurrentInputValues() {
        const form = ctx.root.querySelector("form");
        if (!form) return {};

        const configVar = form.querySelector('input[name="config_variable"]');
        const keyInput = form.querySelector('input[name="key"]');
        const commandInputs = form.querySelectorAll('.command-input');

        return {
          config_variable: configVar ? configVar.value : null,
          key: keyInput ? keyInput.value : null,
          commands: Array.from(commandInputs).map(input => input.value)
        };
      }

      function attachEventListeners() {
        const form = ctx.root.querySelector("form");

        // Handle basic field updates
        form.querySelectorAll('input[name="config_variable"], input[name="key"]').forEach(input => {
          input.addEventListener("change", (event) => {
            state[event.target.name] = event.target.value;
            updateServer();
          });
        });

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
        const currentValues = getCurrentInputValues();
        if (currentValues.config_variable !== null) state.config_variable = currentValues.config_variable;
        if (currentValues.key !== null) state.key = currentValues.key;
        if (currentValues.commands && currentValues.commands.length > 0) state.commands = currentValues.commands;
        updateServer();
      });

      // Initial render
      render();
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
      padding: 0.5rem;
      border: 1px solid #d1d5db;
      border-radius: 0.375rem;
      font-size: 0.875rem;
      line-height: 1.25rem;
    }

    .input:focus {
      outline: none;
      border-color: #3b82f6;
      box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1);
    }

    .help {
      margin-top: 0.25rem;
      font-size: 0.75rem;
      color: #6b7280;
    }

    .commands-container {
      margin-bottom: 0.75rem;
    }

    .command-row {
      margin-bottom: 0.5rem;
    }

    .command-input-group {
      display: flex;
      gap: 0.5rem;
      align-items: center;
    }

    .command-input {
      flex: 1;
      font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
    }

    .button {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      padding: 0.375rem 0.75rem;
      border: 1px solid transparent;
      border-radius: 0.375rem;
      font-size: 0.875rem;
      font-weight: 500;
      cursor: pointer;
      transition: all 0.15s ease-in-out;
      text-decoration: none;
      background-color: #f9fafb;
      color: #374151;
      gap: 0.25rem;
    }

    .button:hover {
      background-color: #f3f4f6;
    }

    .button.is-danger {
      background-color: #ef4444;
      color: white;
    }

    .button.is-danger:hover {
      background-color: #dc2626;
    }

    .button.is-small {
      padding: 0.25rem 0.5rem;
      font-size: 0.75rem;
    }

    .icon {
      font-weight: bold;
      font-size: 1rem;
      line-height: 1;
    }

    /* Dark mode styles */
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

      .button {
        background-color: #374151;
        color: #f3f4f6;
      }

      .button:hover {
        background-color: #4b5563;
      }
    }
    """
  end
end
