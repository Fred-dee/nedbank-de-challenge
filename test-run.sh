#!/usr/bin/env bash
#docker run --rm   --network=none --memory=2g --memory-swap=2g  --cpus=2 --read-only --tmpfs /tmp:rw,size=512m  -v /tmp/test-data:/data  my-submission:test
docker run --rm \
  --network=none \
  --memory=2g --memory-swap=2g \
  --cpus=2 \
  --read-only \
  --tmpfs /tmp:rw,size=512m \
  -v /tmp/test-data:/data \
  my-submission:test

echo "Exit code: $?"