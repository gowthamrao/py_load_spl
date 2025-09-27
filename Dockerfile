# ---- Base Image ----
# Use a consistent base image for both builder and final stage
FROM python:3.11-slim as base

# Set environment variables to prevent writing .pyc files and to buffer output
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1


# ---- Builder Stage ----
# This stage builds the dependencies
FROM base as builder

# Set the working directory
WORKDIR /app

# Install pdm for dependency management
RUN pip install pdm

# Copy only the necessary files for dependency resolution
COPY pyproject.toml pdm.lock ./

# Install project dependencies and export them to requirements.txt
# This includes production dependencies only
RUN pdm install --prod --no-lock && pdm export -o requirements.txt --prod


# ---- Final Stage ----
# This stage creates the final, lean image
FROM base as final

# Set the working directory
WORKDIR /app

# Create a non-root user and group
RUN addgroup --system nonroot && adduser --system --ingroup nonroot nonroot

# Copy the application source code
COPY ./src ./src

# Copy the requirements from the builder stage
COPY --from=builder /app/requirements.txt .

# Install the production dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Change ownership of the app directory to the non-root user
RUN chown -R nonroot:nonroot /app

# Switch to the non-root user
USER nonroot

# Set the entrypoint to run the CLI application
ENTRYPOINT ["python", "-m", "src.py_load_spl.cli"]