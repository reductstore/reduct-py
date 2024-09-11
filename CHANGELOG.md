# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added:

- RS-418: `Bucket.remove_record`, `Bucket.remove_batch` and `Bucket.remove_query` to remove records, [PR-114](https://github.com/reductstore/reduct-py/pull/114)
- RS-289: QuotaType.HARD, [PR-115](https://github.com/reductstore/reduct-py/pulls/115)

## [1.11.0] - 2024-08-19

### Added:

- RS-31: `Bucket.update` and `Bucket.update_batch` method for changing labels, [PR-113](https://github.com/reductstore/reduct-py/pull/113)

## [1.10.0] - 2024-06-11

### Added:

- RS-261: support `each_n` and `each_s` query parameters, [PR-110](https://github.com/reductstore/reduct-py/pull/110)
- RS-311: support `each_s` and `each_n` replication settings, [PR-111](https://github.com/reductstore/reduct-py/pull/111)

### Changed:

- RS-269: move documentation to main website, [PR-112](https://github.com/reductstore/reduct-py/pull/112)

## [1.9.1] - 2024-04-26

## Fixed:

- Timestamps in batch writing, [PR-109](https://github.com/reductstore/reduct-py/pull/109)

## [1.9.0] - 2024-03-08

### Added:

- RS-182: flag `verify_ssl` to Client, [PR-102](https://github.com/reductstore/reduct-py/pull/102)
- RS-176: license field to ServerInfo, [PR-105](https://github.com/reductstore/reduct-py/pull/105)
- RS-196: accept timestamps in many formats, [PR-196](https://github.com/reductstore/reduct-py/pull/106)

### Changed:

- RS-197: update docs for `Record.read` method, [PR-108](https://github.com/reductstore/reduct-py/pull/108)

## [1.8.1] - 2024-01-31

### Fixed:

- Remove duplicated `ReplicationDeailSettings` [PR-101](https://github.com/reductstore/reduct-py/pull/101)

## [1.8.0] - 2024-01-24

### Added:

- Support for ReductStore HTTP API v1.8 with replication endpoints, [PR-100](https://github.com/reductstore/reduct-py/pull/100)

### Changed:

- docs: update link to new website, [PR-98](https://github.com/reductstore/reduct-py/pull/98)
- Optimize batch read for better memory efficiency, [PR-99](https://github.com/reductstore/reduct-py/pull/99)

## [1.7.1] - 2023-10-09

### Fix:

- `Batch` in `__init__.py`, [PR-97](https://github.com/reductstore/reduct-py/pull/97)

## [1.7.0] - 2023-10-06

### Added:

- Support for ReductStore HTTP API v1.7, see `Bucket.write_batch` method, [PR-95](https://github.com/reductstore/reduct-py/pull/95)

### Changed:

- Update dependencies and migrate to Pydantic v2, [PR-94](https://github.com/reductstore/reduct-py/pull/94)

## [1.6.0] - 2023-08-15

### Added:

- External session and context manager to Client, [PR-90](https://github.com/reductstore/reduct-py/pull/90)
- `Bucket.remove_entry` method and `limit` kwarg to `Bucket.query`, [PR-92](https://github.com/reductstore/reduct-py/pull/92)

## [1.5.0] - 2023-06-30

### Added:

- Support for batched records, [PR-78](https://github.com/reductstore/reduct-py/pull/78)
- `extra_headers` to Client constructor, [PR-81](https://github.com/reductstore/reduct-py/pull/81)
- `head` option to `Bucket.query` and `Bucket.read` to read only metadata, [PR-83](https://github.com/reductstore/reduct-py/pull/83)
- `head` option to `Bucket.subscribe`, [PR-87](https://github.com/reductstore/reduct-py/pull/87)

### Fixed:

- Unordered reading batched records, [PR-82](https://github.com/reductstore/reduct-py/pull/82)

### Changed:

- Minimum Python version is now 3.8, [PR-84](https://github.com/reductstore/reduct-py/pull/84)
- Parse short header syntax for batched records, [PR-85](https://github.com/reductstore/reduct-py/pull/85)

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

[Unreleased]: https://github.com/reduct-storage/reduct-py/compare/v1.11.0...HEAD

[1.11.0]: https://github.com/reduct-storage/reduct-py/compare/v1.10.0...v1.11.0

[1.10.0]: https://github.com/reduct-storage/reduct-py/compare/v1.9.1...v1.10.0

[1.9.1]: https://github.com/reduct-storage/reduct-py/compare/v1.9.0...v1.9.1

[1.9.0]: https://github.com/reduct-storage/reduct-py/compare/v1.8.1...v1.9.0

[1.8.1]: https://github.com/reduct-storage/reduct-py/compare/v1.8.0...v1.8.1

[1.8.0]: https://github.com/reduct-storage/reduct-py/compare/v1.7.1...v1.8.0

[1.7.1]: https://github.com/reduct-storage/reduct-py/compare/v1.7.0...v1.7.1

[1.7.0]: https://github.com/reduct-storage/reduct-py/compare/v1.6.0...v1.7.0

[1.6.0]: https://github.com/reduct-storage/reduct-py/compare/v1.5.0...v1.6.0

[1.5.0]: https://github.com/reduct-storage/reduct-py/compare/v1.4.1...v1.5.0

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
