# Repository Guidelines

## Project Structure & Module Organization
- `pkg/reduct`: Core async SDK built on `aiohttp`; `client.py` is the entry point, `bucket.py` and `record.py` manage data access, and `http.py` wraps request logic.
- `pkg/reduct/msg`: Pydantic models for server payloads (buckets, tokens, replication, server info).
- `tests`: Pytest suite (async) mirroring public APIs; fixtures in `conftest.py`.
- `build` and `dist` (when created): Local build artifacts; avoid committing.

## Build, Test, and Development Commands
- Install dev stack: `python -m pip install -U pip setuptools wheel` then `pip install -e .[test,lint,format]`.
- Format check: `black . --check`.
- Lint: `pylint pkg/reduct tests`.
- Run unit/integration tests (expects ReductStore reachable at `http://localhost:8383`): `pytest tests`.
- Build wheel: `pipx run build --wheel` (mirrors CI).
- Optional: start a local ReductStore for tests `docker run -p 8383:8383 -d reduct/store:latest`.

## Coding Style & Naming Conventions
- **Python 3.10+ compatibility required**: Code must be compatible with Python 3.10 and make use of its features (e.g., use PEP 604 union syntax `X | Y` instead of `Union[X, Y]`, use `X | None` instead of `Optional[X]`).
- Follow Black defaults (88 cols) and PEP8; prefer explicit `async`/`await` patterns and context managers for HTTP clients.
- Module, function, and variable names use `snake_case`; classes use `PascalCase`; constants are `SCREAMING_SNAKE_CASE`.
- Keep public API stable; favor small helper functions over inline duplication.
- When adding request/response shapes, define Pydantic models in `pkg/reduct/msg/` and reuse them in clients to keep validation consistent.

## Testing Guidelines
- Tests live in `tests/*_test.py`; mirror API surface and include async paths.
- Use `pytest-asyncio` fixtures; avoid real network calls beyond the local ReductStore container.
- Environment knobs for integration runs: `RS_API_TOKEN` (token auth), `RS_LICENSE_PATH` (license file path). Set them in the test command when needed.
- Prefer scenario-based tests that assert HTTP error handling via `pytest.raises` and status codes exposed by `error.py`.
- **Always test changes with both stable (`reduct/store:latest`) and development (`reduct/store:main`) versions of ReductStore** to ensure compatibility with upcoming features and avoid breaking changes.
  - Start the development version: `docker run -p 8383:8383 -d reduct/store:main`
  - Start the stable version: `docker run -p 8383:8383 -d reduct/store:latest`

## Commit & Pull Request Guidelines
- Commit messages are short and imperative; include issue/PR numbers when relevant (e.g., `Fix crash on non-ReductStore response (#141)`).
- Before opening a PR, run format, lint, and tests; include what changed, why, and how it was verified. Add repro steps or screenshots for user-facing behavior.
- Update `CHANGELOG.md` for notable behavior changes or new API surface.
  - One PR should have one record in CHANGELOG.md
  - Include the PR link in the format `[PR-XXX](https://github.com/reductstore/reduct-py/pull/XXX)` at the end of the changelog entry
- Keep PRs focused; if touching public API, mention migration notes in the description or docstrings.
