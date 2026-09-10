#!/usr/bin/env bash
set -euo pipefail

cargo nextest run \
  -p krabka-schema-serde \
  --test capture_jvm_fixtures \
  -E 'test(=capture_jvm_serde_fixtures)' \
  --run-ignored only \
  --no-tests fail \
  --test-threads 1
