# CI/CD Strategy for py-load-spl

## 1. Overview

This document outlines the architecture, tools, and best practices for the Continuous Integration and Continuous Deployment (CI/CD) pipeline for the `py-load-spl` project. The primary goals of this pipeline are to ensure code quality, maintain security, and automate the testing and build processes efficiently.

The strategy is built on the principles of fast feedback, comprehensive testing, and security hardening at every stage.

## 2. Technology Stack and Rationale

The following tools have been selected to build a robust and modern CI/CD pipeline:

-   **Dependency Management:** `pdm` is used for its modern features, adherence to PEP standards, and integrated workflow management. It provides a single, reliable tool for managing dependencies and running scripts.
-   **Continuous Integration:** `GitHub Actions` is used as the CI platform for its tight integration with the source code repository and its extensive ecosystem of community-supported actions.
-   **Code Quality:** `pre-commit` is used to orchestrate a suite of linters and formatters, including `Ruff`, `Black`, and `Mypy`. This ensures that all code committed to the repository adheres to a consistent style and quality standard.
-   **Testing:** `pytest` is used for its powerful features, extensive plugin ecosystem, and clear test organization.
-   **Containerization:** `Docker` is used to create a portable and reproducible runtime environment for the application.
-   **Security Scanning:** `Trivy` is integrated to scan Docker images for known vulnerabilities, ensuring the security of the final application artifact.

## 3. Proactive Improvements Made

Several foundational improvements were made to the repository to establish a best-in-class CI/CD pipeline:

-   **Standardized on `pdm`:** Confirmed `pdm` as the sole dependency manager and ensured all configurations align with its usage.
-   **Created `.dockerignore`:** A comprehensive `.dockerignore` file was added to minimize the Docker build context, resulting in faster and more efficient image builds by excluding files like `.git`, `__pycache__`, and `.venv`.
-   **Refactored `Dockerfile`:** The `Dockerfile` was completely refactored to use a multi-stage build. This separates the build environment from the final runtime environment, creating a lean, secure, and production-ready image. The final image now runs as a non-root user to enhance security.
-   **Implemented Comprehensive `pre-commit` Hooks:** A `.pre-commit-config.yaml` file was created to automate code quality checks. It includes hooks for `Ruff` (linting and formatting), `Mypy` (static type checking), and other repository hygiene checks.

## 4. Workflow Architecture

The CI/CD pipeline is composed of two separate GitHub Actions workflows: `ci.yml` and `docker.yml`.

-   **`ci.yml`:** This workflow focuses on code quality and correctness. It is structured in two stages to provide fast feedback:
    1.  **`lint` Job:** This job runs first and executes `pre-commit` to perform linting, formatting, and static analysis. A failure here immediately notifies the developer without wasting time on tests.
    2.  **`test` Job:** This job depends on the success of the `lint` job. It runs the full test suite across a matrix of operating systems and Python versions.

-   **`docker.yml`:** This workflow is responsible for building and scanning the Docker image. It runs in parallel with `ci.yml` and provides assurance that the application can be containerized and is free of critical vulnerabilities.

## 5. Testing Strategy

The testing strategy is designed to be comprehensive and provide clear insights into code coverage.

-   **Test Matrix:** The `test` job in `ci.yml` runs on a matrix of:
    -   **Operating Systems:** `ubuntu-latest`, `macos-latest`, `windows-latest`
    -   **Python Versions:** `3.11`, `3.12`
-   **Test Separation:** Tests are separated into `unit` and `integration` tests using `pytest` markers. This allows them to be run and reported on separately.
-   **Code Coverage:** `Codecov` is used to track test coverage. Coverage reports for unit and integration tests are uploaded with distinct flags (e.g., `ubuntu-latest-py312-unit`), providing a granular view of test coverage across different environments.

## 6. Dependency Management and Caching

-   **PDM Installation:** `pdm` is installed in the CI environment using `pipx`. This ensures that `pdm` is installed in an isolated environment and is available on the `PATH`, preventing "command not found" errors.
-   **GitHub Actions Cache:** The `docker/build-push-action` is configured to use the GitHub Actions cache (`type=gha`) to speed up Docker image builds by reusing layers from previous runs.

## 7. Security Hardening

Security is a core component of this CI/CD pipeline, with the following measures in place:

-   **Principle of Least Privilege (PoLP):** All workflow jobs are configured with `permissions: contents: read` to limit their access to the repository.
-   **Action Pinning:** All third-party GitHub Actions are pinned to their full commit SHA to prevent supply chain attacks.
-   **Non-Root Docker User:** The `Dockerfile` creates and switches to a non-root user (`nonroot`) before running the application, reducing the attack surface.
-   **Docker Hub Authentication:** The `docker.yml` workflow securely logs into Docker Hub using secrets to prevent rate-limiting issues when pulling base images.
-   **Vulnerability Scanning:** The `Trivy` action scans the built Docker image for `CRITICAL` and `HIGH` severity vulnerabilities and will fail the build if any are found.

## 8. Docker Strategy

-   **Multi-Stage Builds:** The `Dockerfile` uses a multi-stage build to create a minimal final image. The `builder` stage installs `pdm` and exports the dependencies to a `requirements.txt` file. The `final` stage copies the application code and installs dependencies from the `requirements.txt` file, without including `pdm` or other build-time tools.
-   **Build Caching:** The `docker.yml` workflow uses the GitHub Actions cache as a remote cache for Docker layers, significantly speeding up subsequent builds.
-   **Verification, Not Pushing:** The Docker image is built and loaded locally for scanning but is not pushed to a registry on pull requests. This ensures the image can be built successfully without polluting the container registry.

## 9. How to Run Locally

The CI checks can be easily replicated locally to catch issues before committing code.

-   **Run Pre-commit Checks:**
    ```bash
    # Install pre-commit hooks
    pre-commit install
    # Run all checks on all files
    pre-commit run --all-files
    ```

-   **Run Tests:**
    ```bash
    # Install all dependencies, including dev
    pdm install -d
    # Run all tests
    pdm run pytest
    # Run only unit tests
    pdm run pytest -m "not integration"
    # Run only integration tests
    pdm run pytest -m "integration"
    ```
-   **Build the Docker Image:**
    ```bash
    docker build -t py-load-spl .
    ```