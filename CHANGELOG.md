# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added:

- Support Python 3.7, [PR-53](https://github.com/reduct-storage/reduct-py/pull/53)
- `Client.get_full_info()` to get full information about a
  bucket, [pr-55](https://github.com/reduct-storage/reduct-py/pull/55)

### Changed:

- `Client.get_bucket` now uses `GET` instead of `HEAD` in order to be able to return a meaningful error to the
  user, [PR-51](https://github.com/reduct-storage/reduct-py/pull/51)
- `Client` class now catches parsing errors raised by incorrect server configurations or missing
  servers, [PR-52](https://github.com/reduct-storage/reduct-py/pull/52)

### Fixed:

- Fix examples in docstrings, [PR-54](https://github.com/reduct-storage/reduct-py/pull/54)

## [v1.0.0] - 2022-10-18

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

[Unreleased]: https://github.com/reduct-storage/reduct-py/compare/v0.5.1...HEAD

[0.5.1]: https://github.com/reduct-storage/reduct-py/compare/v0.4.0...v0.5.1

[0.4.0]: https://github.com/reduct-storage/reduct-py/compare/v0.3.0...v0.4.0

[0.3.0]: https://github.com/reduct-storage/reduct-py/compare/v0.2.0...v0.3.0

[0.2.0]: https://github.com/reduct-storage/reduct-py/compare/v0.1.0...v0.2.0

[0.1.0]: https://github.com/reduct-storage/reduct-py/compare/tag/v0.1.0
