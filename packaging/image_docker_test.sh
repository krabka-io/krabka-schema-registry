#!/usr/bin/env bash
# Runs the registry image through a real Docker daemon.
#
# //packaging:image_binaries_test runs the binary on the host. This test runs
# it under the loader and libc of the apko base, so it finds a binary that
# links against a library the base does not carry. It needs a daemon, so the
# target is `manual`:
#
#     bazel test -c opt //packaging:image_docker_test
#
# Argument 1 is the `image_load` runner. Argument 2 is the tag it loads the
# image under. //packaging:image_docker_test passes both.
set -euo pipefail

loader="$1"
tag="$2"

if ! docker info >/dev/null 2>&1; then
    echo "image_docker_test: no reachable Docker daemon." >&2
    echo "  Start Docker, or run //packaging:image_binaries_test, which checks" >&2
    echo "  the layer without one." >&2
    exit 1
fi

# The loader finds its own runfiles, which Bazel merges into the runfiles of
# this test.
"${loader}"

# No `--entrypoint`: this is what a bare `docker run` and the Helm chart start.
if ! docker run --rm "${tag}" --help >/dev/null; then
    echo "image_docker_test: the image entrypoint did not answer --help" >&2
    exit 1
fi

echo "image_docker_test: the entrypoint of ${tag} answered --help"
