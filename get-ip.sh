#!/usr/bin/env bash
# Script to get the IP address of the container
# Usage: ./get-postgres-ip.sh

set -eou pipefail

# Usage function
if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <container_name>"
    exit 1
fi

container_name="$1"
# Check if a container with this name exists
if ! docker ps -a --format '{{.Names}}' | grep -q "^${container_name}\$"; then
    echo "Container with name ${container_name} does not exist"
    exit 1
fi

docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ${container_name}

# END