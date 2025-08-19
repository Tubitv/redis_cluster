![RedisCluster](img/redis-cluster-logo-with-text.png)

[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg?style=flat-square)](https://tubitv.hexdocs.pm/redis_cluster/)
[![CI](https://github.com/Tubitv/redis_cluster/actions/workflows/ci.yml/badge.svg)](https://github.com/Tubitv/redis_cluster/actions/workflows/ci.yml)

[![Run in Livebook](https://livebook.dev/badge/v1/black.svg)](https://livebook.dev/run?url=https%3A%2F%2Fgithub.com%2FTubitv%2Fredis_cluster%2Fblob%2Fmain%2Fnotebooks%2Fredis-cluster-demo.livemd)

`RedisCluster` is an Elixir library that extends the popular [Redix](https://hex.pm/packages/redix) library to provide seamless support for [Redis Cluster](https://redis.io/topics/cluster-spec). It handles cluster topology discovery, request routing to the correct nodes, and connection pooling, allowing you to interact with a Redis Cluster with the simplicity of a single Redis instance.

This library is built to be robust and performant, leveraging Redix's efficient Redis protocol implementation and OTP principles for supervision and concurrency.

## Features

### Automatic Cluster Discovery

Dynamically discovers and maintains the cluster topology, including master and replica nodes.

### Smart Command Routing

Intelligently routes commands to the appropriate node based on the key's hash slot, transparently handling `MOVED` redirections.

### Connection Pooling

Manages a configurable pool of connections to each cluster node for optimal performance and resource utilization. See `RedisCluster.Pool`.

### Redix Integration

Built on top of Redix, inheriting its reliability and speed for Redis communication.

### Telemetry Support

Emits comprehensive telemetry events for monitoring, metrics, and debugging. See `RedisCluster.Telemetry`.

### Pipeline Operations

Supports pipelining of commands for improved performance with batch operations.

### Resharding Awareness

Adapts to cluster changes, such as resharding, by updating its internal slot map.

### Multi-Get/Set

Conveniently provides basic get/set across many nodes.

## Demo

To try out `RedisCluster` check out the [demo notebook](notesbooks/redis_cluster-demo.livemd).

[![Run in Livebook](https://livebook.dev/badge/v1/black.svg)](https://livebook.dev/run?url=https%3A%2F%2Fgithub.com%2FTubitv%2Fredis_cluster%2Fblob%2Fmain%2Fnotebooks%2Fredis-cluster-demo.livemd)

## Installation

Add `redis_cluster` to your list of dependencies in `mix.exs`:

```elixir
defp deps do
  [
    {:redis_cluster, "~> 0.3.2", organization: "tubitv"},
  ]
end
```

## Configuration

There are two ways to configure `RedisCluster`: a robust, production-ready solution, or a quick and easy solution.

### Production Ready Config

First, you need to create a module for your Redis use:

```elixir
defmodule MyApp.Redis do
  use RedisCluster, otp_app: :my_app
end
```

Then in your `config/runtime.exs` you can set the details for your environment:

```elixir
redis_host = System.get_env("REDIS_HOST", "localhost")
redis_port = System.get_env("REDIS_PORT", "6379") |> String.to_integer()

config :my_app, MyApp.Redis,
  host: redis_host,
  port: redis_port,
  pool_size: 16
```

Ideally the host should be a "configuration endpoint" as AWS ElastiCache calls it. The configuration endpoint picks a random node to connect to. This ensures one node isn't being hit every time the cluster needs to be discovered.

### Quick and Easy Config

For simpler cases, such as with testing and Livebook, you can inline your config.

```elixir
defmodule MyApp.Redis do
  use RedisCluster, otp_app: :none,
    host: "localhost",
    port: 6379,
    pool_size: 3
end
```
