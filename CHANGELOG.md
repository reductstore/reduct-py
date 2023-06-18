# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added:

- Support for batched records, [PR-78](https://github.com/reductstore/reduct-py/pull/78)
- `extra_headers` to Client constructor, [PR-81](https://github.com/reductstore/reduct-py/pull/81)
- `head` option to `Bucket.query` and `Bucket.read` to read only metadata, [PR-83](https://github.com/reductstore/reduct-py/pull/83)

### Fixed:

- Unordered reading batched records, [PR-82](https://github.com/reductstore/reduct-py/pull/82)

### Changed:

- Minimum Python version is now 3.8, [PR-84](https://github.com/reductstore/reduct-py/pull/84)

## [1.4.1] - 2023-06-05

### Fixed:

- Force close connection after each request to avoid
  ServerDisconnectedError, [PR-77](https://github.com/reductstore/reduct-py/pull/77)

## [1.4.0] - 2023-05-29

### Added:

- Subscribing to new records, [PR-70](https://github.com/reductstore/reduct-py/pull/70)

### Fixed:

- No int timestamps in queries, [PR-73](https://github.com/reductstore/reduct-py/pull/73)
- Use Expect: 100-continue for large files, [PR-75](https://github.com/reductstore/reduct-py/pull/75)

## [1.3.1] - 2023-01-30

### Fixed:

- No content status in `Bucket.query`, [PR-69](https://github.com/reductstore/reduct-py/pull/69)

## [1.3.0] - 2023-01-27

### Added:

- Quick Start example and guide, [PR-65](https://github.com/reductstore/reduct-py/pull/65)
- Support labels for read, write and querying, [PR-66](https://github.com/reductstore/reduct-py/pull/66)
- `Content-Type` header for read and write operations, [PR-67](https://github.com/reductstore/reduct-py/pull/67)

## [1.2.0] - 2022-12-22

### Added:

- Client.me() method to get current permissions, [PR-62](https://github.com/reductstore/reduct-py/pull/62)

### Changed:

- Update documentation after rebranding, [PR-59](https://github.com/reductstore/reduct-py/pull/59)
- Migrate to pyproject.toml, [PR-61](https://github.com/reductstore/reduct-py/pull/61)

## [1.1.0] - 2022-11-29

### Added:

- Support Python 3.7, [PR-53](https://github.com/reduct-storage/reduct-py/pull/53)
- `Client.get_full_info()` to get full information about a
  bucket, [pr-55](https://github.com/reduct-storage/reduct-py/pull/55)
- Implement token API for Reduct Storage API v1.1, [PR-56](https://github.com/reduct-storage/reduct-py/pull/56)

### Changed:

- `Client.get_bucket` now uses `GET` instead of `HEAD` in order to be able to return a meaningful error to the
  user, [PR-51](https://github.com/reduct-storage/reduct-py/pull/51)
- `Client` class now catches parsing errors raised by incorrect server configurations or missing
  servers, [PR-52](https://github.com/reduct-storage/reduct-py/pull/52)

### Fixed:

- Fix examples in docstrings, [PR-54](https://github.com/reduct-storage/reduct-py/pull/54)

## [1.0.0] - 2022-10-18

### Added:

- `/api/v1/` prefix to all http endpoints, [PR-42](https://github.com/reduct-storage/reduct-py/pull/42)

### Changed:

- `bucket.read()` now returns a Record yielded from an
  asynccontext, [PR-43](https://github.com/reduct-storage/reduct-py/pull/43)

### Removed:

- Deprecated entry `list` function, [PR-42](https://github.com/reduct-storage/reduct-py/pull/42)
- `bucket.read_by` method, [PR-43](https://github.com/reduct-storage/reduct-py/pull/43)

## [0.5.1] - 2022-09-14

### Removed:

- Token renewal - deprecated in API v0.8, [PR-39](https://github.com/reduct-storage/reduct-py/pull/39)

## [0.4.0] - 2022-08-24

### Added:

- Support HTTP API v0.7, [PR-36](https://github.com/reduct-storage/reduct-py/pull/36)

### Fixed:

- Content length can be 0 with async version of command, [PR-37](https://github.com/reduct-storage/reduct-py/pull/37)
- Type hint in bucket.write incorrect, [PR-37](https://github.com/reduct-storage/reduct-py/pull/37)

## [0.3.0] - 2022-07-02

### Added:

- Support HTTP API v0.6, [PR-28](https://github.com/reduct-storage/reduct-py/pull/28)
- Timeout for HTTP request, [PR-29](https://github.com/reduct-storage/reduct-py/pull/29)
- Streaming data, [PR-30](https://github.com/reduct-storage/reduct-py/pull/30)

### Fixed:

- Exceptions print status code and server message, [PR-32](https://github.com/reduct-storage/reduct-py/pull/32)

### Changed:

- Update dependencies, [PR-27](https://github.com/reduct-storage/reduct-py/pull/27)

## [0.2.0] - 2022-06-12

### Added:

- `exist_ok` flag to Client.create_bucket, [PR-21](https://github.com/reduct-storage/reduct-py/pull/21)
- Support HTTP API v0.5, [PR-22](https://github.com/reduct-storage/reduct-py/pull/22)

### Changed:

- Documentation for `Bucket.list` method, [PR-20](https://github.com/reduct-storage/reduct-py/pull/20)

## [0.1.0] - 2022-05-21

- Implement Reduct Storage HTTP API v0.4, [PR-16](https://github.com/reduct-storage/reduct-py/pull/16)

[Unreleased]: https://github.com/reduct-storage/reduct-py/compare/v1.4.1...HEAD

[1.4.1]: https://github.com/reduct-storage/reduct-py/compare/v1.4.0...v1.4.1

[1.4.0]: https://github.com/reduct-storage/reduct-py/compare/v1.3.1...v1.4.0

[1.3.1]: https://github.com/reduct-storage/reduct-py/compare/v1.3.0...v1.3.1

[1.3.0]: https://github.com/reduct-storage/reduct-py/compare/v1.2.0...v1.3.0

[1.2.0]: https://github.com/reduct-storage/reduct-py/compare/v1.1.0...v1.2.0

[1.1.0]: https://github.com/reduct-storage/reduct-py/compare/v1.0.0...v1.1.0

[1.0.0]: https://github.com/reduct-storage/reduct-py/compare/v0.5.1...v1.0.0

[0.5.1]: https://github.com/reduct-storage/reduct-py/compare/v0.4.0...v0.5.1

[0.4.0]: https://github.com/reduct-storage/reduct-py/compare/v0.3.0...v0.4.0

[0.3.0]: https://github.com/reduct-storage/reduct-py/compare/v0.2.0...v0.3.0

[0.2.0]: https://github.com/reduct-storage/reduct-py/compare/v0.1.0...v0.2.0

[0.1.0]: https://github.com/reduct-storage/reduct-py/compare/tag/v0.1.0
