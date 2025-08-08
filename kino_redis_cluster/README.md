# KinoRedisCluster

A collection of [Livebook](https://livebook.dev/) smart cells for working with Redis Cluster using the [RedisCluster](https://hex.pm/packages/redis_cluster) library.

## Features

### Redis Cluster Connect Smart Cell

- **Interactive connection setup**: Configure cluster name, host, port, and pool size through a user-friendly interface
- **Input validation**: Real-time validation of connection parameters
- **Configuration output**: Returns a `RedisCluster.Configuration` struct for use in subsequent cells

### Redis Cluster Pipeline Smart Cell

- **Dynamic command builder**: Add, edit, and remove Redis commands with an intuitive UI
- **Pipeline execution**: Execute multiple Redis commands in a single pipeline for optimal performance
- **Key routing**: Specify keys for hash slot routing or use `:any` for node-agnostic commands
- **Command validation**: Validates command format and structure

## Installation

Add `kino_redis_cluster` to your Livebook setup cell:

```elixir
Mix.install([
  {:kino_redis_cluster, github: "your-org/redis_cluster", sparse: "kino_redis_cluster"}
])
```

## Usage

1. **Create a connection**: Use the "RedisCluster: Connect" smart cell to establish a connection to your Redis cluster
2. **Run commands**: Use the "RedisCluster: Pipeline" smart cell to execute Redis commands

The smart cells are automatically registered when the application starts and will appear in Livebook's smart cell menu.

## Development

To work on this project:

1. Clone the repository
2. Navigate to the `kino_redis_cluster` directory
3. Run `mix deps.get` to install dependencies
4. Open `notebooks/smart_cells_demo.livemd` in Livebook to test the smart cells

## Example

See `notebooks/smart_cells_demo.livemd` for a complete example of using both smart cells together.