defmodule Livebook.SmartCell.RedisCluster.Pipeline do
  use Kino.JS
  use Kino.JS.Live
  use Kino.SmartCell, name: "RedisCluster: Run Pipeline"

  @impl true
  def init(attrs, ctx) do
    config_variable = attrs["config_variable"] || "config"
    key = attrs["key"] || "foo"
    commands = attrs["commands"] || ~s{[["SET", "key", "value"], ["GET", "key"]]}

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
    broadcast_event(ctx, "update", %{
      config_variable: ctx.assigns.config_variable,
      key: ctx.assigns.key,
      commands: ctx.assigns.commands
    })
    {:noreply, ctx}
  end

  @impl true
  def to_attrs(ctx) do
    %{
      "config_variable" => ctx.assigns.config_variable,
      "key" => ctx.assigns.key,
      "commands" => ctx.assigns.commands
    }
  end

  @impl true
  def to_source(attrs) do
    config_var = attrs["config_variable"]
    key = attrs["key"]
    commands = attrs["commands"]

    # Validate and parse commands
    case parse_and_validate_commands(commands) do
      {:ok, parsed_commands} ->
        # Determine key parameter based on input
        key_param = case String.trim(key) do
          "any" -> ":any"
          ":any" -> ":any"
          other_key -> inspect(other_key)
        end

        code = """
        # Redis Cluster Pipeline Commands
        commands = #{format_commands_for_output(parsed_commands)}

        #{config_var} |> RedisCluster.Cluster.pipeline(commands, #{key_param})
        """

        {:ok, code}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Private helper functions

  defp parse_and_validate_commands(commands_json) do
    case Jason.decode(commands_json) do
      {:ok, commands} when is_list(commands) ->
        validated_commands =
          Enum.map(commands, fn
            cmd when is_list(cmd) -> cmd
            cmd when is_binary(cmd) -> String.split(cmd, ~r/\s+/)
            _ -> ["INVALID"]
          end)

        if Enum.any?(validated_commands, &(&1 == ["INVALID"])) do
          {:error, "Invalid command format"}
        else
          {:ok, validated_commands}
        end

      {:error, _} ->
        # Try to parse as a simple string and convert to command list
        try do
          commands_list = [String.split(String.trim(commands_json), ~r/\s+/)]
          {:ok, commands_list}
        rescue
          _ -> {:error, "Invalid commands format. Expected JSON array of commands."}
        end
    end
  end

  defp format_commands_for_output(commands) do
    commands
    |> Enum.map(fn cmd ->
      cmd
      |> Enum.map(&inspect/1)
      |> Enum.join(", ")
      |> then(&"[#{&1}]")
    end)
    |> Enum.join(",\n  ")
    |> then(&"[\n  #{&1}\n]")
  end

  asset "main.js" do
    """
    export function init(ctx, payload) {
      ctx.importCSS("main.css");

      let state = {
        config_variable: payload.config_variable || 'config',
        key: payload.key || 'foo',
        commands: payload.commands || '[["SET", "key", "value"], ["GET", "key"]]'
      };

      function renderCommands() {
        let commands = [];
        try {
          commands = JSON.parse(state.commands);
        } catch (e) {
          commands = [["GET", "key"]];
        }

        return commands.map((cmd, index) => {
          const cmdStr = Array.isArray(cmd) ? cmd.join(' ') : cmd;
          return `
            <div class="command-row" data-index="${index}">
              <div class="command-input-group">
                <input
                  class="input command-input"
                  type="text"
                  value="${cmdStr}"
                  placeholder="SET key value"
                  data-index="${index}"
                >
                <button type="button" class="button is-danger is-small remove-command" data-index="${index}">
                  <span class="icon">×</span>
                </button>
              </div>
            </div>
          `;
        }).join('');
      }

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
              <input class="input" type="text" name="key" value="${state.key}" placeholder="foo">
              <p class="help">Redis key for hash slot routing (use :any for commands that work on any node)</p>
            </div>

            <div class="field">
              <label class="label">Pipeline Commands</label>
              <div class="commands-container">
                ${renderCommands()}
              </div>
              <button type="button" class="button is-primary is-small add-command">
                <span class="icon">+</span>
                <span>Add Command</span>
              </button>
              <p class="help">Redis commands to execute in pipeline. Each command should be space-separated (e.g., "SET key value")</p>
            </div>
          </form>
        `;

        ctx.root.innerHTML = formHtml;
        attachEventListeners();
      }

      function attachEventListeners() {
        const form = ctx.root.querySelector("form");

        // Handle basic field updates
        form.querySelectorAll('input[name="config_variable"], input[name="key"]').forEach(input => {
          input.addEventListener("input", (event) => {
            const field = event.target.name;
            const value = event.target.value;
            const updates = {};
            updates[field] = value;
            ctx.pushEvent("update", updates);
          });
        });

        // Handle command input changes
        form.querySelectorAll('.command-input').forEach(input => {
          input.addEventListener("input", (event) => {
            updateCommands();
          });
        });

        // Handle add command button
        const addButton = form.querySelector('.add-command');
        if (addButton) {
          addButton.addEventListener('click', () => {
            let commands = [];
            try {
              commands = JSON.parse(state.commands);
            } catch (e) {
              commands = [];
            }

            commands.push(["GET", "key"]);
            state.commands = JSON.stringify(commands);
            ctx.pushEvent("update", { commands: state.commands });
            render();
          });
        }

        // Handle remove command buttons
        form.querySelectorAll('.remove-command').forEach(button => {
          button.addEventListener('click', (event) => {
            const index = parseInt(event.target.closest('.remove-command').dataset.index);
            let commands = [];
            try {
              commands = JSON.parse(state.commands);
            } catch (e) {
              commands = [];
            }

            commands.splice(index, 1);
            if (commands.length === 0) {
              commands.push(["GET", "key"]);
            }

            state.commands = JSON.stringify(commands);
            ctx.pushEvent("update", { commands: state.commands });
            render();
          });
        });
      }

      function updateCommands() {
        const commandInputs = ctx.root.querySelectorAll('.command-input');
        const commands = [];

        commandInputs.forEach(input => {
          const cmdStr = input.value.trim();
          if (cmdStr) {
            const parts = cmdStr.split(/\s+/);
            commands.push(parts);
          }
        });

        if (commands.length === 0) {
          commands.push(["GET", "key"]);
        }

        state.commands = JSON.stringify(commands);
        ctx.pushEvent("update", { commands: state.commands });
      }

      // Handle server updates
      ctx.handleEvent("update", (payload) => {
        state = { ...state, ...payload };
        render();
      });

      // Handle synchronization
      ctx.handleSync(() => {
        // Trigger change events on all inputs to ensure state is synchronized
        const inputs = ctx.root.querySelectorAll('input');
        inputs.forEach(input => {
          if (document.activeElement === input) {
            input.dispatchEvent(new Event('input'));
          }
        });
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
      font-family: 'SF Mono', Monaco, 'Cascadia Code', 'Roboto Mono', Consolas, 'Courier New', monospace;
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

    .button.is-primary {
      background-color: #3b82f6;
      color: white;
    }

    .button.is-primary:hover {
      background-color: #2563eb;
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
