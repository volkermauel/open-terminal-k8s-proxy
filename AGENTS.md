# Agent Instructions

## Pre-commit Checks

Always run linting and tests before committing changes:

- **Linting**: `ruff check .` and `mypy terminal_proxy`
- **Tests**: `pytest tests -v --tb=short`

## Pull Request Attributions

When a pull request is merged, add an entry to the attributions table in `README.md` before committing any follow-up changes.

Command to check for newly merged PRs:
```
git log --merges --format='%H %s' origin/main
```
