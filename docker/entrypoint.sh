#!/usr/bin/env bash
set -e

if [ -f /workspace/wayang/mvnw ]; then
    sed -i 's/\r$//' /workspace/wayang/mvnw
    chmod +x /workspace/wayang/mvnw
fi

exec "$@"