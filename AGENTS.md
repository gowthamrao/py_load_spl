# Agent Instructions for py-load-spl

This document provides instructions for AI agents working on this codebase.

## Project Overview

`py-load-spl` is a Python package for extracting, transforming, and loading FDA Structured Product Labeling (SPL) data into a relational database. It is designed to be high-performance, extensible, and maintainable.

## Development Workflow

1.  **Dependencies:** This project uses `pdm` for dependency management. Use `pdm install` to set up the environment.
2.  **Code Style:** Code is formatted with `Ruff` and `Black`. Please run `pdm run ruff format .` before committing.
3.  **Linting & Type Checking:** All code must pass `pdm run ruff .` and `pdm run mypy .` with zero errors. `MyPy` is run in `strict` mode.
4.  **Testing:** All new code must be accompanied by tests. The full test suite is run with `pdm run pytest`. The goal is >95% test coverage. Use `pdm run pytest --cov` to check.
5.  **Database Tests:** Integration tests use `testcontainers` to spin up a real PostgreSQL database. Ensure you have Docker running to execute these tests.

## Key Architectural Principles

-   **Adapter Pattern:** The `db.base.DatabaseLoader` defines an abstract interface for database operations. All database-specific logic should be implemented in an adapter class (e.g., `db.postgres.PostgresLoader`).
-   **Configuration:** All configuration is managed via `config.py` using `Pydantic V2`. Do not hardcode values.
-   **Extensibility:** The transformation layer is designed to be extensible. When adding new output formats, follow the existing patterns to create new writer classes.
