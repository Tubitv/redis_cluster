# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

None

## [0.7.0] - 2025-08-12

### Changed
- Removed global `:default_role` configuration. Read operations now always default to `:master` unless `:role` is explicitly passed to a function. This reverts behavior to pre-0.6.x to avoid developer surprise when reads hit replicas.

### Docs
- Updated option docs in `RedisCluster.Cluster` to reflect the default role change and remove references to global override.

## [0.6.1] - 2025-08-09

### Added
- Livebook smart cells for Redis Cluster operations (#5)
- Async command support in Livebook
- Pipeline cell for batch operations
- Connect cell for cluster connections
- Configurable default role for connections

## [0.6.0] - 2025-08-04

### Added
- MONITOR support for real-time command monitoring (#4)
- Comprehensive test coverage for monitoring options
- Queue-based monitoring with configurable limits
- Filtering with reservoir sampling option

## [0.5.0] - 2025-07-30

### Added
- Parallel versions of `get_many`, `set_many`, and `delete_many` functions
- `broadcast_async` function for asynchronous broadcasting
- Performance optimizations for batch operations
- Support for optional reply suppression to improve throughput
- Timeout configuration option exposed at top level

### Changed
- Optimized loops and removed unnecessary inner work
- Extracted common code patterns
- Moved conditionals outside of loops for better performance

### Added
- MOVED telemetry event support with `set_many*` operations
- Warnings about stale data in documentation

## [0.4.0] - 2025-07-28

### Added
- ASK redirect handling for cluster resharding scenarios
- Redis stubbing for improved testing
- Comprehensive tests for ASK redirects

### Changed
- Updated documentation for all modules
- Made key parameter required in relevant functions

### Fixed
- Test failures related to redirect handling
- Dialyzer errors

## [0.3.4] - 2025-07-02

### Added
- More debug information in logs for troubleshooting

## [0.3.3] - 2025-07-01

### Added
- Warning logs when rediscovering the cluster

### Fixed
- Compile errors

## [0.3.2] - 2025-06-10

### Fixed
- Issue with `get_many` for non-string keys
- Source URL consistency

### Changed
- Updated version in README
- Livebook link updated
- Code formatting improvements

### Added
- Ignore Redis dump files in git

## [0.3.1] - 2025-06-03

### Fixed
- Error with bang (!) functions
- CI badge display
- CI pipeline issues (#1)

### Changed
- Extended wait time for cluster startup
- Made source URL consistent

## [0.3.0] - 2025-05-30

### Added
- Telemetry collection and events for monitoring
- Support for telemetry metrics

## [0.2.1] - 2025-05-30

### Fixed
- Test host reference issues

### Changed
- Default to master node for connections
- Updated Livebook integration

## [0.2.0] - 2025-05-21

### Added
- Standalone Redis support (non-cluster mode)
- Support for connecting to local, standalone replicas

### Fixed
- Proper cleanup of one-off connections

## [0.1.1] - 2025-05-16

### Fixed
- Configuration issues with runtime.exs
- Duplicated string in codebase

## [0.1.0] - 2025-05-16

### Added
- Initial release with Redis Cluster support
- Read replica support with automatic read-only mode
- Connection identification by host, port, and role
- Support for multiple replicas per master
- Core Redis commands: `GET`, `SET`, `DEL`, `MSET`, `MGET`
- Batch operations: `get_many`, `set_many`, `delete_many`
- Broadcasting functionality for pub/sub
- Node discovery and automatic cluster topology updates
- Comprehensive documentation
- Livebook integration for interactive demos
- Dialyzer support for type checking
- Proper CLI flag handling
- Clean file management options

### Features
- Extends Redix with Redis cluster support
- Automatic handling of MOVED and ASK redirects
- Connection pooling
- Read replica load balancing
- Cluster node discovery
- Health monitoring

[Unreleased]: https://github.com/Tubitv/redis_cluster/compare/v0.7.0...HEAD
[0.7.0]: https://github.com/Tubitv/redis_cluster/compare/v0.6.1...v0.7.0
[0.6.1]: https://github.com/Tubitv/redis_cluster/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/Tubitv/redis_cluster/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/Tubitv/redis_cluster/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/Tubitv/redis_cluster/compare/v0.3.4...v0.4.0
[0.3.4]: https://github.com/Tubitv/redis_cluster/compare/v0.3.3...v0.3.4
[0.3.3]: https://github.com/Tubitv/redis_cluster/compare/v0.3.2...v0.3.3
[0.3.2]: https://github.com/Tubitv/redis_cluster/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/Tubitv/redis_cluster/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/Tubitv/redis_cluster/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/Tubitv/redis_cluster/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/Tubitv/redis_cluster/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/Tubitv/redis_cluster/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/Tubitv/redis_cluster/releases/tag/v0.1.0
