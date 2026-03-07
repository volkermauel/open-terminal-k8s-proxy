# Agent Instructions

## Pre-commit Checks

Always run linting and tests before committing changes:

- **Linting**: `ruff check .` and `mypy terminal_proxy`
- **Tests**: `pytest tests -v --tb=short`
