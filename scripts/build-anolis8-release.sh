#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: 2026 Beryl Contributors

set -Eeuo pipefail
IFS=$'\n\t'
umask 022

readonly RELEASE_TARGET="x86_64-unknown-linux-gnu"
readonly EXPECTED_RUSTUP_RELEASE="1.28.2"
readonly EXPECTED_RUST_RELEASE="1.95.0"
readonly EXPECTED_PROTOC_RELEASE="21.12"
readonly MAX_GLIBC_VERSION="GLIBC_2.28"
readonly BUILDER_OS_VERSION="8.8"
readonly BUILDER_IMAGE_REPOSITORY="localhost/beryl/anolis8-release-builder"

die() {
    printf 'error: %s\n' "$*" >&2
    exit 1
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

usage() {
    cat <<EOF
Usage: $(basename -- "$0") [--check-host]

Build Beryl release binaries in the pinned Anolis ${BUILDER_OS_VERSION} builder.

  --check-host  Validate the Anolis 8 rootless Podman host, then exit.
EOF
}

mode="build"
case ${1:-} in
    "")
        ;;
    --check-host)
        mode="check-host"
        ;;
    --help|-h)
        usage
        exit 0
        ;;
    *)
        usage >&2
        die "unsupported argument: $1"
        ;;
esac
[[ $# -le 1 ]] || die "only one argument is supported"

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
readonly script_dir
repo_root=$(cd -- "${script_dir}/.." && pwd -P)
readonly repo_root
readonly containerfile="${repo_root}/release/anolis8/Containerfile"
readonly repository_definition="${repo_root}/release/anolis8/anolis-8.8.repo"
readonly builder_context="${repo_root}/release/anolis8"

for command in awk df getconf hostname podman stat uname; do
    require_command "${command}"
done

[[ ${EUID} -ne 0 ]] || die "run this script as a non-root user with rootless Podman"
[[ $(uname -s) == "Linux" ]] || die "the release build host must run Linux"
[[ $(uname -m) == "x86_64" ]] || die "the release build host must be x86_64"
[[ -r /etc/os-release ]] || die "cannot read /etc/os-release"

# shellcheck source=/dev/null
source /etc/os-release
[[ ${ID:-} == "anolis" ]] || die "the release build host must run Anolis OS"
[[ ${VERSION_ID%%.*} == "8" ]] || die "the release build host must run Anolis OS 8"
[[ $(getconf GNU_LIBC_VERSION) == "glibc 2.28" ]] \
    || die "the release build host must use glibc 2.28"

[[ -n ${XDG_RUNTIME_DIR:-} ]] \
    || die "XDG_RUNTIME_DIR is unset; log in directly as the non-root build user"
[[ -d ${XDG_RUNTIME_DIR} && -w ${XDG_RUNTIME_DIR} ]] \
    || die "XDG_RUNTIME_DIR is not a writable directory: ${XDG_RUNTIME_DIR}"
[[ $(stat -c '%u' "${XDG_RUNTIME_DIR}") == "${EUID}" ]] \
    || die "XDG_RUNTIME_DIR is not owned by uid ${EUID}: ${XDG_RUNTIME_DIR}"

podman_rootless=$(podman info --format '{{.Host.Security.Rootless}}') \
    || die "Podman is not usable by the current user"
[[ ${podman_rootless} == "true" ]] \
    || die "Podman must run rootless; log in as the non-root build user"
readonly podman_rootless

for command in git install mktemp sha256sum; do
    require_command "${command}"
done

host_os_version=${VERSION_ID}
host_arch=$(uname -m)
host_kernel=$(uname -r)
host_glibc=$(getconf GNU_LIBC_VERSION)
host_podman_version=$(podman --version)
host_available_kib=$(df -Pk "${repo_root}" | awk 'NR == 2 { print $4 }')
[[ ${host_available_kib} =~ ^[0-9]+$ ]] || die "cannot determine available build disk space"
readonly host_os_version host_arch host_kernel host_glibc host_podman_version host_available_kib

printf 'Release build host preflight passed.\n'
printf 'Host: Anolis %s, %s, %s, %s\n' \
    "${host_os_version}" "${host_arch}" "${host_kernel}" "${host_glibc}"
printf 'Container engine: %s (rootless)\n' "${host_podman_version}"
printf 'Available workspace disk: %s GiB\n' "$((host_available_kib / 1024 / 1024))"

if [[ ${mode} == "check-host" ]]; then
    exit 0
fi

[[ -f ${containerfile} ]] || die "builder definition not found: ${containerfile}"
[[ -f ${repository_definition} ]] || die "builder repository definition not found: ${repository_definition}"
[[ -f ${repo_root}/Cargo.toml ]] || die "workspace Cargo.toml not found"
[[ -f ${repo_root}/Cargo.lock ]] || die "workspace Cargo.lock not found"

actual_repo_root=$(git -C "${repo_root}" rev-parse --show-toplevel)
[[ ${actual_repo_root} == "${repo_root}" ]] || die "script is not running from the Beryl repository root"

if [[ -n $(git -C "${repo_root}" status --porcelain=v1 --untracked-files=normal) ]]; then
    die "release builds require a clean Git worktree"
fi

source_revision=$(git -C "${repo_root}" rev-parse HEAD)
[[ ${source_revision} =~ ^[0-9a-fA-F]{40}$|^[0-9a-fA-F]{64}$ ]] \
    || die "Git HEAD is not a full hexadecimal object identifier"
readonly source_revision

source_date_epoch=$(git -C "${repo_root}" show -s --format=%ct HEAD)
[[ ${source_date_epoch} =~ ^[0-9]+$ ]] || die "Git commit time is not a Unix timestamp"
readonly source_date_epoch

workspace_version=$(
    awk '
        $0 == "[workspace.package]" { in_workspace_package = 1; next }
        in_workspace_package && /^\[/ { exit }
        in_workspace_package && /^version = "/ {
            sub(/^version = "/, "")
            sub(/"$/, "")
            print
            exit
        }
    ' "${repo_root}/Cargo.toml"
)
[[ -n ${workspace_version} ]] || die "cannot resolve workspace package version"
readonly workspace_version

containerfile_sha=$(sha256sum "${containerfile}" | awk '{print $1}')
repository_definition_sha=$(sha256sum "${repository_definition}" | awk '{print $1}')
cargo_lock_sha=$(sha256sum "${repo_root}/Cargo.lock" | awk '{print $1}')
readonly containerfile_sha repository_definition_sha cargo_lock_sha

builder_definition_key="${containerfile_sha:0:12}-${repository_definition_sha:0:12}"
builder_image_tag="${BUILDER_IMAGE_REPOSITORY}:anolis-${BUILDER_OS_VERSION}-${builder_definition_key}"
readonly builder_definition_key builder_image_tag

builder_iid_file=$(mktemp "${TMPDIR:-/tmp}/beryl-anolis8-builder-iid.XXXXXX")
readonly builder_iid_file
cleanup() {
    rm -f -- "${builder_iid_file}"
}
trap cleanup EXIT

printf 'Building the pinned Anolis %s release builder...\n' "${BUILDER_OS_VERSION}"
podman build \
    --format docker \
    --platform linux/amd64 \
    --pull=missing \
    --file "${containerfile}" \
    --iidfile "${builder_iid_file}" \
    --tag "${builder_image_tag}" \
    "${builder_context}"

builder_image_id=$(<"${builder_iid_file}")
if [[ ${builder_image_id} =~ ^[0-9a-f]{64}$ ]]; then
    builder_image_id="sha256:${builder_image_id}"
fi
[[ ${builder_image_id} =~ ^sha256:[0-9a-f]{64}$ ]] \
    || die "Podman returned an invalid builder image ID: ${builder_image_id}"
readonly builder_image_id

base_image=$(podman image inspect \
    --format '{{ index .Config.Labels "org.beryl.release.base-image" }}' \
    "${builder_image_id}")
[[ ${base_image} =~ ^[^[:space:]]+@sha256:[0-9a-f]{64}$ ]] \
    || die "builder image does not declare an immutable Anolis base image"
readonly base_image

builder_os=$(podman image inspect \
    --format '{{ index .Config.Labels "org.beryl.release.builder-os" }}' \
    "${builder_image_id}")
[[ ${builder_os} == "anolis-${BUILDER_OS_VERSION}" ]] \
    || die "builder OS label does not match Anolis ${BUILDER_OS_VERSION}: ${builder_os}"
readonly builder_os

builder_target=$(podman image inspect \
    --format '{{ index .Config.Labels "org.beryl.release.target" }}' \
    "${builder_image_id}")
[[ ${builder_target} == "${RELEASE_TARGET}" ]] \
    || die "builder target label does not match ${RELEASE_TARGET}: ${builder_target}"
readonly builder_target

builder_rust=$(podman image inspect \
    --format '{{ index .Config.Labels "org.beryl.release.rust" }}' \
    "${builder_image_id}")
[[ ${builder_rust} == "${EXPECTED_RUST_RELEASE}" ]] \
    || die "builder Rust label does not match ${EXPECTED_RUST_RELEASE}: ${builder_rust}"
readonly builder_rust

builder_rustup=$(podman image inspect \
    --format '{{ index .Config.Labels "org.beryl.release.rustup" }}' \
    "${builder_image_id}")
[[ ${builder_rustup} == "${EXPECTED_RUSTUP_RELEASE}" ]] \
    || die "builder rustup label does not match ${EXPECTED_RUSTUP_RELEASE}: ${builder_rustup}"
readonly builder_rustup

builder_protoc=$(podman image inspect \
    --format '{{ index .Config.Labels "org.beryl.release.protoc" }}' \
    "${builder_image_id}")
[[ ${builder_protoc} == "${EXPECTED_PROTOC_RELEASE}" ]] \
    || die "builder protoc label does not match ${EXPECTED_PROTOC_RELEASE}: ${builder_protoc}"
readonly builder_protoc

builder_key=${builder_image_id#sha256:}
builder_key=${builder_key:0:16}
readonly builder_key
readonly build_root="${repo_root}/target/anolis8-release/${source_revision}/${builder_key}"
readonly cargo_target_dir="${build_root}/cargo-target"
readonly cargo_home="${repo_root}/target/anolis8-cargo-home"
readonly artifacts_dir="${build_root}/artifacts"

mkdir -p "${cargo_target_dir}" "${cargo_home}" "${artifacts_dir}"

cat >"${build_root}/build-host.txt" <<EOF
hostname=$(hostname)
os_id=${ID}
os_version=${host_os_version}
arch=${host_arch}
kernel=${host_kernel}
glibc=${host_glibc}
podman=${host_podman_version}
podman_rootless=${podman_rootless}
available_workspace_kib=${host_available_kib}
EOF

printf 'Building Beryl release binaries from %s...\n' "${source_revision}"
podman run --rm \
    --platform linux/amd64 \
    --security-opt label=disable \
    --env "BERYL_SOURCE_REVISION=${source_revision}" \
    --env "SOURCE_DATE_EPOCH=${source_date_epoch}" \
    --env "CARGO_HOME=/cargo-home" \
    --env "CARGO_TARGET_DIR=/cargo-target" \
    --volume "${repo_root}:/workspace:ro" \
    --volume "${cargo_home}:/cargo-home:rw" \
    --volume "${cargo_target_dir}:/cargo-target:rw" \
    --workdir /workspace \
    "${builder_image_id}" \
    cargo build \
        --release \
        --locked \
        --target "${RELEASE_TARGET}" \
        --package beryl-cli \
        --package beryl-metadata \
        --package beryl-worker

readonly cargo_release_dir="${cargo_target_dir}/${RELEASE_TARGET}/release"
for binary in beryl beryl-metadata beryl-worker; do
    [[ -x ${cargo_release_dir}/${binary} ]] \
        || die "expected release binary was not built: ${binary}"
    install -m 0755 "${cargo_release_dir}/${binary}" "${artifacts_dir}/${binary}"
done

printf 'Validating ELF, glibc, shared-library, and build-identity contracts...\n'
podman run --rm \
    --platform linux/amd64 \
    --security-opt label=disable \
    --env "EXPECTED_REVISION=${source_revision}" \
    --env "EXPECTED_VERSION=${workspace_version}" \
    --env "EXPECTED_TARGET=${RELEASE_TARGET}" \
    --env "EXPECTED_RUST_RELEASE=${EXPECTED_RUST_RELEASE}" \
    --env "MAX_GLIBC_VERSION=${MAX_GLIBC_VERSION}" \
    --volume "${artifacts_dir}:/artifacts:ro" \
    "${builder_image_id}" \
    /bin/bash -Eeuo pipefail -c '
        for name in beryl beryl-metadata beryl-worker; do
            binary="/artifacts/${name}"
            file "${binary}" | grep -q "ELF 64-bit LSB.*x86-64"

            version_output=$("${binary}" --version)
            grep -Fqx "${name} ${EXPECTED_VERSION}" <<<"${version_output}"
            grep -Fqx "source-revision: ${EXPECTED_REVISION}" <<<"${version_output}"
            grep -Fqx "target: ${EXPECTED_TARGET}" <<<"${version_output}"
            grep -Fq "rustc: rustc ${EXPECTED_RUST_RELEASE} " <<<"${version_output}"

            ldd_output=$(ldd "${binary}")
            if grep -q "not found" <<<"${ldd_output}"; then
                printf "unresolved runtime dependency for %s:\n" "${name}" >&2
                printf "%s\n" "${ldd_output}" >&2
                exit 1
            fi

            max_glibc=$(
                readelf --version-info "${binary}" \
                    | sed -n "s/.*Name: \(GLIBC_[0-9][0-9.]*\).*/\1/p" \
                    | sort -Vu \
                    | tail -n 1
            )
            [[ -n ${max_glibc} ]]
            if [[ $(printf "%s\n%s\n" "${max_glibc}" "${MAX_GLIBC_VERSION}" | sort -Vu | tail -n 1) != "${MAX_GLIBC_VERSION}" ]]; then
                printf "%s requires %s, above the %s baseline\n" \
                    "${name}" "${max_glibc}" "${MAX_GLIBC_VERSION}" >&2
                exit 1
            fi
            printf "%s: %s, max-glibc=%s\n" "${name}" "${EXPECTED_TARGET}" "${max_glibc}"
        done
    '

# Running in the pinned base image catches accidental dependencies on compiler
# or development libraries that exist only in the builder image.
for binary in beryl beryl-metadata beryl-worker; do
    podman run --rm \
        --platform linux/amd64 \
        --security-opt label=disable \
        --volume "${artifacts_dir}:/artifacts:ro" \
        "${base_image}" \
        "/artifacts/${binary}" --version >/dev/null
done

cat >"${build_root}/build-environment.txt" <<EOF
source_revision=${source_revision}
source_date_epoch=${source_date_epoch}
version=${workspace_version}
target=${RELEASE_TARGET}
builder_os=${builder_os}
builder_image_id=${builder_image_id}
builder_image_tag=${builder_image_tag}
base_image=${base_image}
rustup_release=${builder_rustup}
protoc_release=${builder_protoc}
containerfile_sha256=${containerfile_sha}
repository_definition_sha256=${repository_definition_sha}
cargo_lock_sha256=${cargo_lock_sha}
EOF

podman run --rm \
    --platform linux/amd64 \
    "${builder_image_id}" \
    /bin/bash -Eeuo pipefail -c '
        cat /etc/os-release
        printf "\n"
        getconf GNU_LIBC_VERSION
        rustup --version
        rustc --version --verbose
        cargo --version
        protoc --version
        cc --version | sed -n "1p"
        c++ --version | sed -n "1p"
        clang --version | sed -n "1p"
        cmake --version | sed -n "1p"
    ' >"${build_root}/toolchain.txt"

podman run --rm \
    --platform linux/amd64 \
    "${builder_image_id}" \
    rpm -qa --qf '%{NAME}-%{EPOCH}:%{VERSION}-%{RELEASE}.%{ARCH}\n' \
    | LC_ALL=C sort >"${build_root}/builder-rpms.txt"

builder_rpms_sha=$(sha256sum "${build_root}/builder-rpms.txt" | awk '{print $1}')
printf 'builder_rpms_sha256=%s\n' "${builder_rpms_sha}" >>"${build_root}/build-environment.txt"

printf '\nRelease build completed successfully.\n'
printf 'Artifacts: %s\n' "${artifacts_dir}"
printf 'Build environment: %s\n' "${build_root}/build-environment.txt"
printf 'Build host: %s\n' "${build_root}/build-host.txt"
printf 'Toolchain inventory: %s\n' "${build_root}/toolchain.txt"
printf 'RPM inventory: %s\n' "${build_root}/builder-rpms.txt"
