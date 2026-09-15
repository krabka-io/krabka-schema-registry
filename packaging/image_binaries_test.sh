#!/usr/bin/env bash
# Checks the registry image without a Docker daemon.
#
# The image base has no shell, so the image can run only what its layers carry.
# This test asserts four things about the built artifacts:
#
#   * the layers put exactly `krabka-schema-registry` under /usr/bin,
#   * that binary runs and answers `--help`,
#   * the image manifest references every layer, so a layer that is no longer
#     in `layers` fails here and not in a registry,
#   * the binary is an ELF for the architecture that the manifest declares.
#     The layer holds what the host toolchain built, and the manifest
#     architecture is a constant, so on a host that is not amd64 the two
#     disagree. //packaging:image is restricted to x86_64 for that reason.
#
# Argument 1 is the architecture that the manifest declares (IMAGE_ARCH in
# //packaging/BUILD.bazel). Argument 2 is the image manifest JSON. The rest are
# the layer tarballs. //packaging:image_binaries_test passes all of them.
set -euo pipefail

expected=(krabka-schema-registry)

arch="$1"
manifest="$2"
shift 2
layers=("$@")

fail() {
    echo "image_binaries_test: $*" >&2
    exit 1
}

# `e_machine`, the two bytes at offset 0x12 of an ELF header, little-endian.
# `readelf` is not in the Bazel test sandbox, and `od` is.
case "${arch}" in
    amd64) want_machine="3e00" ;;
    arm64) want_machine="b700" ;;
    *) fail "no ELF e_machine known for architecture ${arch}" ;;
esac

machine_name() {
    case "$1" in
        3e00) echo "amd64" ;;
        b700) echo "arm64" ;;
        "") echo "not an ELF file" ;;
        *) echo "unknown e_machine 0x$1" ;;
    esac
}

# The two bytes at 0x12 in hex, or nothing for a file with no ELF magic.
elf_machine() {
    if [[ "$(od -An -tx1 -N 4 "$1" | tr -d ' \n')" != "7f454c46" ]]; then
        return 0
    fi
    od -An -tx1 -j 18 -N 2 "$1" | tr -d ' \n'
}

sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | cut -d' ' -f1
    else
        shasum -a 256 "$1" | cut -d' ' -f1
    fi
}

work="${TEST_TMPDIR:-$(mktemp -d)}"
rootfs="${work}/rootfs"
mkdir -p "${rootfs}"

[[ -f "${manifest}" ]] || fail "image manifest ${manifest} is not a file"

for layer in "${layers[@]}"; do
    [[ -f "${layer}" ]] || fail "layer ${layer} is not a file"
    tar -xf "${layer}" -C "${rootfs}"

    # rules_img stores a layer as the gzipped tarball that the rule writes, so
    # the blob digest in the manifest is the digest of that file.
    digest="$(sha256 "${layer}")"
    if ! grep -qF "sha256:${digest}" "${manifest}"; then
        fail "the image does not reference layer ${layer} (sha256:${digest})"
    fi
done

shipped="$(find "${rootfs}/usr/bin" -type f -exec basename {} \; | LC_ALL=C sort | tr '\n' ' ')"
want="$(printf '%s\n' "${expected[@]}" | LC_ALL=C sort | tr '\n' ' ')"
if [[ "${shipped}" != "${want}" ]]; then
    fail "/usr/bin holds [${shipped}], expected [${want}]"
fi

for binary in "${expected[@]}"; do
    path="${rootfs}/usr/bin/${binary}"
    [[ -x "${path}" ]] || fail "${binary} is not executable in the image"
    found_machine="$(elf_machine "${path}")"
    if [[ "${found_machine}" != "${want_machine}" ]]; then
        fail "/usr/bin/${binary} is $(machine_name "${found_machine}"), but the image manifest declares ${arch}"
    fi
    status=0
    "${path}" --help >/dev/null 2>&1 || status=$?
    [[ "${status}" -eq 0 ]] || fail "${binary} --help exited ${status}"
done

echo "image_binaries_test: ${#expected[@]} ${arch} binary under /usr/bin, answering --help"
