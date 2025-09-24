# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the project files
COPY . .

# Install PDM
RUN pip install pdm

# Install dependencies
# Using --prod to not install dev dependencies
RUN pdm install --prod

# The entrypoint is set to run the CLI application.
# Users of the image will need to pass arguments to the container
# e.g., docker run <image_name> init
ENTRYPOINT ["pdm", "run", "py-load-spl"]
