#!/bin/sh
set -e

mc alias set local "${MINIO_ENDPOINT}" "${MINIO_ACCESS_KEY}" "${MINIO_SECRET_KEY}"

for bucket in bronze silver gold mlops-artifacts; do
    mc mb --ignore-existing "local/${bucket}"
    echo "Bucket '${bucket}' ready"
done
