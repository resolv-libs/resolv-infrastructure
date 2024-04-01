#!/bin/bash

# Define the directory containing the subdirectories with Docker Compose files
BASE_DIR="."

source ./.env

# Iterate over each subdirectory
for dir in "$BASE_DIR"/*; do
    if [ -d "$dir" ]; then
        echo "Stopping Docker Compose in $dir..."
        docker-compose --project-directory .. -f "$dir/docker-compose.yaml" stop
    fi
done
