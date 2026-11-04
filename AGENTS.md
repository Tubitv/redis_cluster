# AGENTS.md

Guidance for AI coding agents working in this repo.

## What this is

`redis_cluster` is an Elixir library (published to Hex, repo `Tubitv/redis_cluster`) that adds Redis Cluster support on top of [Redix](https://hex.pm/packages/redix): topology discovery, slot-based routing, per-node connection pools, `MOVED`/`ASK` handling, and telemetry.

It must work with plain Redis/Valkey clusters, AWS ElastiCache (cluster mode), and ElastiCache Serverless for Valkey.

## Layout

| Path | Purpose |
|---|---|
| `lib/redis_cluster.ex` | `use RedisCluster` macro that generates the `get/set/delete/command/pipeline/...` wrapper functions |
| `lib/redis_cluster/cluster.ex` | Main API: commands, pipelines, `*_many`, retry and `MOVED`/`ASK` handling |
| `lib/redis_cluster/configuration.ex` | `%Configuration{}` struct, app-env loading, `redix_opts/1` (builds Redix connect options, including TLS) |
| `lib/redis_cluster/shard_discovery.ex` | GenServer that discovers the topology and (re)builds pools |
| `lib/redis_cluster/cluster_info.ex` | Runs `CLUSTER SHARDS`, falling back to `CLUSTER SLOTS`, then standalone `ROLE` |
| `lib/redis_cluster/cluster/*_parser.ex` | Parsers for `CLUSTER SHARDS`, `CLUSTER SLOTS`, `ROLE`; `node_info.ex` is the parsed node struct |
| `lib/redis_cluster/pool.ex`, `connection.ex` | Connection pools per node; replicas get `READONLY` (re-sent after Redix reconnects) |
| `lib/redis_cluster/hash_slots.ex`, `key.ex` | ETS slot table and CRC16 hash slot / hash tag logic |
| `lib/redis_cluster/monitor.ex` | Debug `MONITOR` client (opens its own socket with `:ssl`/`:gen_tcp`) |
| `lib/redis_cluster/telemetry.ex` | Telemetry events (documented in the module) |
| `scripts/redis_cluster.exs` | Starts/stops a local Redis cluster for tests |
| `kino_redis_cluster/` | Separate Mix project (Livebook smart cell); has its own deps and is not covered by the root `mix test` |
| `notebooks/` | Livebook demo |

## Running the tests

**The test suite needs a real local Redis cluster on ports 6379-6390. Without it, about 15 tests fail** (`ClusterTest`, `ShardDiscoveryTest`, `MonitorTest`, `RedisClusterTest`). A standalone Redis on 6379 is not enough. A passing run is the only acceptable result; do not treat those failures as "expected".

```bash
# 1. Free port 6379: stop anything already listening (a local Redis, or a container publishing 6379)
lsof -nP -iTCP:6379 -sTCP:LISTEN   # must print nothing

# 2. Start the cluster exactly as CI does (12 nodes: 3 masters x 3 replicas each)
./scripts/redis_cluster.exs start --port 6379 --replicas-per-master 3
sleep 10
redis-cli -p 6379 cluster info | head -3     # expect cluster_state:ok, cluster_slots_assigned:16384

# 3. Run the tests
mix test                                      # expect: 25 doctests, 107 tests, 0 failures
mix test test/redis_cluster/cluster/          # single dir/file also works (parser tests need no Redis)

# 4. Stop the cluster when finished
./scripts/redis_cluster.exs stop --port 6379
./scripts/redis_cluster.exs stop --port 6379 --purge-files   # also delete data/logs in $TMPDIR/redis
```

Notes:
- `--port 6379` is the first port. The script uses 12 consecutive ports (6379-6390). The script's default is 9000, which the tests do not use, so always pass `--port 6379`.
- `--replicas-per-master 3` is what CI uses. The tests expect nodes on 6379-6390 (some assert specific ports such as 6380 and 6390), so other replica counts can fail.
- Use `--dry-run` to print the commands without running them.
- Requires `redis-server` and `redis-cli` on the PATH (`brew install redis`). `redis-cli --cluster create` does the cluster setup.
- The suite takes about 30 seconds. Some suites share the cluster and run serially, so don't run `mix test` twice at once.
- Verify the cluster is healthy before blaming a code change for failures: `redis-cli -p 6379 cluster info`.
- Parser, configuration, pool and cluster-info tests use mocks (`Mox`) and need no Redis.

## Other checks (CI runs these)

```bash
mix format --check-formatted
mix compile --warnings-as-errors --force
mix dialyzer            # CI runs it on the newest Elixir/OTP only; first run builds PLTs and is slow
```

CI matrix: Elixir 1.17-1.18, OTP 25-27 (`.github/workflows/ci.yml`). Keep code compatible with `elixir: "~> 1.14"`.

## Conventions

- Update `CHANGELOG.md` (Keep a Changelog format, `[Unreleased]` section) for any user-visible change. Version lives in `mix.exs` (`@version`).
- Tests that exercise TLS or connection options use a mock `redis_module` (see `pool_test.exs`, `cluster_info_test.exs`) and assert the options handed to `start_link`. Redix connect options are built in one place, `Configuration.redix_opts/1`; don't build them separately in other modules.
- Don't commit `.tool-versions` changes, `plan.md`, `security-audit.markdown`, or `erl_crash.dump` unless asked.

## Gotchas

- **TLS:** Redix needs `ssl: true` plus the TLS options as `socket_opts` (not nested under `:ssl`). ElastiCache certificates are wildcard certs, so use `customize_hostname_check: [match_fun: :public_key.pkix_verify_hostname_match_fun(:https)]` with `verify: :verify_peer` and `cacerts: :public_key.cacerts_get()`.
- **ElastiCache Serverless for Valkey** (verified against Valkey 8.1): one virtual shard with the primary on 6379 and a replica on 6380 (same hostname); TLS is mandatory. `CLUSTER SHARDS` returns `ip 127.0.0.1` and only `tls-port`, with the DNS name in `endpoint`/`hostname`; `ShardParser` handles this. `CONFIG`, `CLIENT ID/INFO`, `MEMORY`, `FUNCTION`, `KEYS`, `SLOWLOG` are blocked. `WATCH`, `MULTI/EXEC`, `EVAL`/`EVALSHA` and `SCAN` work.
- **RESP3:** Redix only parses RESP2. Sending `HELLO 3` crashes the connection process.
- **Cross-slot commands** (`MGET`/`EVAL` with keys in different slots) return `CROSSSLOT`. Use hash tags (`{id}`) with `compute_hash_tag: true`, or the `*_many` helpers, which group by slot.
- A failed discovery makes callers block while it retries and logs "No nodes found for slot ... rediscovering cluster".
