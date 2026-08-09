#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: 2026 Beryl Contributors

set -Eeuo pipefail
IFS=$'\n\t'
umask 022

readonly RELEASE_TARGET="x86_64-unknown-linux-gnu"

die() {
    printf 'error: %s\n' "$*" >&2
    exit 1
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

usage() {
    cat <<EOF
Usage: $(basename -- "$0") --build-root DIR

Package one validated Beryl build as a deterministic Linux tarball.

  --build-root DIR  Build directory produced by build-anolis8-release.sh.
EOF
}

case ${1:-} in
    --build-root)
        [[ $# -eq 2 ]] || die "--build-root requires exactly one directory"
        ;;
    --help|-h)
        usage
        exit 0
        ;;
    "")
        usage >&2
        exit 1
        ;;
    *)
        usage >&2
        die "unsupported argument: $1"
        ;;
esac

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
readonly script_dir
repo_root=$(cd -- "${script_dir}/.." && pwd -P)
readonly repo_root

[[ -d $2 ]] || die "build root is not a directory: $2"
build_root=$(cd -- "$2" && pwd -P)
readonly build_root
readonly build_environment="${build_root}/build-environment.txt"
readonly artifacts_dir="${build_root}/artifacts"
readonly builder_rpms="${build_root}/builder-rpms.txt"
readonly containerfile="${repo_root}/release/anolis8/Containerfile"
readonly repository_definition="${repo_root}/release/anolis8/anolis-8.8.repo"

for command in awk basename chmod cmp git install mkdir mktemp mv podman rm sha256sum uname; do
    require_command "${command}"
done

[[ ${EUID} -ne 0 ]] || die "run this script as the non-root release build user"
[[ $(uname -s) == "Linux" ]] || die "release packaging must run on Linux"
[[ $(uname -m) == "x86_64" ]] || die "release packaging must run on x86_64"

[[ -f ${build_environment} && ! -L ${build_environment} ]] \
    || die "build environment is not a regular file: ${build_environment}"
[[ -d ${artifacts_dir} && ! -L ${artifacts_dir} ]] \
    || die "artifacts directory is missing or is a symbolic link: ${artifacts_dir}"
[[ -f ${builder_rpms} && ! -L ${builder_rpms} ]] \
    || die "builder RPM inventory is not a regular file: ${builder_rpms}"

metadata_value() {
    local key=$1
    awk -v key="${key}" '
        index($0, key "=") == 1 {
            if (found) {
                exit 2
            }
            value = substr($0, length(key) + 2)
            found = 1
        }
        END {
            if (!found) {
                exit 1
            }
            print value
        }
    ' "${build_environment}"
}

source_revision=$(metadata_value source_revision) \
    || die "build environment has no unique source_revision"
source_date_epoch=$(metadata_value source_date_epoch) \
    || die "build environment has no unique source_date_epoch"
version=$(metadata_value version) \
    || die "build environment has no unique version"
target=$(metadata_value target) \
    || die "build environment has no unique target"
builder_os=$(metadata_value builder_os) \
    || die "build environment has no unique builder_os"
builder_image_id=$(metadata_value builder_image_id) \
    || die "build environment has no unique builder_image_id"
base_image=$(metadata_value base_image) \
    || die "build environment has no unique base_image"
rust_release=$(metadata_value rust_release) \
    || die "build environment has no unique rust_release"
rustup_release=$(metadata_value rustup_release) \
    || die "build environment has no unique rustup_release"
protoc_release=$(metadata_value protoc_release) \
    || die "build environment has no unique protoc_release"
containerfile_sha=$(metadata_value containerfile_sha256) \
    || die "build environment has no unique containerfile_sha256"
repository_definition_sha=$(metadata_value repository_definition_sha256) \
    || die "build environment has no unique repository_definition_sha256"
cargo_lock_sha=$(metadata_value cargo_lock_sha256) \
    || die "build environment has no unique cargo_lock_sha256"
beryl_sha=$(metadata_value beryl_sha256) \
    || die "build environment has no unique beryl_sha256"
beryl_metadata_sha=$(metadata_value beryl_metadata_sha256) \
    || die "build environment has no unique beryl_metadata_sha256"
beryl_worker_sha=$(metadata_value beryl_worker_sha256) \
    || die "build environment has no unique beryl_worker_sha256"
builder_rpms_sha=$(metadata_value builder_rpms_sha256) \
    || die "build environment has no unique builder_rpms_sha256"
readonly source_revision source_date_epoch version target builder_os
readonly builder_image_id base_image rust_release rustup_release protoc_release
readonly containerfile_sha repository_definition_sha cargo_lock_sha builder_rpms_sha
readonly beryl_sha beryl_metadata_sha beryl_worker_sha

[[ ${source_revision} =~ ^[0-9a-f]{40}$|^[0-9a-f]{64}$ ]] \
    || die "source_revision is not a full lowercase Git object ID"
[[ ${source_date_epoch} =~ ^[0-9]+$ ]] \
    || die "source_date_epoch is not a Unix timestamp"
[[ ${version} =~ ^[0-9A-Za-z][0-9A-Za-z.+-]*$ ]] \
    || die "version cannot be used in an archive name: ${version}"
[[ ${target} == "${RELEASE_TARGET}" ]] \
    || die "build target is not ${RELEASE_TARGET}: ${target}"
[[ ${builder_os} =~ ^anolis-8\.[0-9]+$ ]] \
    || die "builder_os is not an Anolis 8 release: ${builder_os}"
[[ ${builder_image_id} =~ ^sha256:[0-9a-f]{64}$ ]] \
    || die "builder_image_id is not an immutable image ID"
[[ ${base_image} =~ ^[^[:space:]]+@sha256:[0-9a-f]{64}$ ]] \
    || die "base_image is not an immutable image reference"
for digest in \
    "${containerfile_sha}" \
    "${repository_definition_sha}" \
    "${cargo_lock_sha}" \
    "${beryl_sha}" \
    "${beryl_metadata_sha}" \
    "${beryl_worker_sha}" \
    "${builder_rpms_sha}"; do
    [[ ${digest} =~ ^[0-9a-f]{64}$ ]] || die "build environment contains an invalid SHA-256 digest"
done

actual_repo_root=$(git -C "${repo_root}" rev-parse --show-toplevel)
[[ ${actual_repo_root} == "${repo_root}" ]] || die "script is not running from the Beryl repository root"
if [[ -n $(git -C "${repo_root}" status --porcelain=v1 --untracked-files=normal) ]]; then
    die "release packaging requires a clean Git worktree"
fi
[[ $(git -C "${repo_root}" rev-parse HEAD) == "${source_revision}" ]] \
    || die "build source revision does not match Git HEAD"
[[ $(git -C "${repo_root}" show -s --format=%ct HEAD) == "${source_date_epoch}" ]] \
    || die "build source date does not match the Git commit time"

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
[[ ${workspace_version} == "${version}" ]] \
    || die "build version ${version} does not match workspace version ${workspace_version}"
[[ $(sha256sum "${containerfile}" | awk '{print $1}') == "${containerfile_sha}" ]] \
    || die "Containerfile does not match the build environment"
[[ $(sha256sum "${repository_definition}" | awk '{print $1}') == "${repository_definition_sha}" ]] \
    || die "Anolis repository definition does not match the build environment"
[[ $(sha256sum "${repo_root}/Cargo.lock" | awk '{print $1}') == "${cargo_lock_sha}" ]] \
    || die "Cargo.lock does not match the build environment"
[[ $(sha256sum "${builder_rpms}" | awk '{print $1}') == "${builder_rpms_sha}" ]] \
    || die "builder RPM inventory does not match the build environment"

for package_input in \
    "${repo_root}/conf/metadata.yaml" \
    "${repo_root}/conf/worker.yaml" \
    "${repo_root}/conf/client.yaml" \
    "${repo_root}/release/systemd/beryl-metadata.service" \
    "${repo_root}/release/systemd/beryl-worker.service" \
    "${repo_root}/release/logrotate/beryl" \
    "${repo_root}/release/tmpfiles/beryl.conf" \
    "${repo_root}/release/install.sh" \
    "${repo_root}/LICENSE" \
    "${repo_root}/OPERATIONS.md" \
    "${repo_root}/README.md"; do
    [[ -f ${package_input} && ! -L ${package_input} ]] \
        || die "package input is not a regular file: ${package_input}"
done

for binary in beryl beryl-metadata beryl-worker; do
    [[ -f ${artifacts_dir}/${binary} && ! -L ${artifacts_dir}/${binary} && -x ${artifacts_dir}/${binary} ]] \
        || die "release artifact is not a regular executable: ${artifacts_dir}/${binary}"
done
[[ $(sha256sum "${artifacts_dir}/beryl" | awk '{print $1}') == "${beryl_sha}" ]] \
    || die "beryl artifact does not match the build environment"
[[ $(sha256sum "${artifacts_dir}/beryl-metadata" | awk '{print $1}') == "${beryl_metadata_sha}" ]] \
    || die "beryl-metadata artifact does not match the build environment"
[[ $(sha256sum "${artifacts_dir}/beryl-worker" | awk '{print $1}') == "${beryl_worker_sha}" ]] \
    || die "beryl-worker artifact does not match the build environment"

podman image inspect "${builder_image_id}" >/dev/null \
    || die "builder image is unavailable: ${builder_image_id}"
podman run --rm \
    --platform linux/amd64 \
    --security-opt label=disable \
    --env "EXPECTED_REVISION=${source_revision}" \
    --env "EXPECTED_VERSION=${version}" \
    --env "EXPECTED_TARGET=${target}" \
    --env "EXPECTED_RUST_RELEASE=${rust_release}" \
    --volume "${artifacts_dir}:/artifacts:ro" \
    "${builder_image_id}" \
    /bin/bash -Eeuo pipefail -c '
        for name in beryl beryl-metadata beryl-worker; do
            output=$("/artifacts/${name}" --version)
            grep -Fqx "${name} ${EXPECTED_VERSION}" <<<"${output}"
            grep -Fqx "source-revision: ${EXPECTED_REVISION}" <<<"${output}"
            grep -Fqx "target: ${EXPECTED_TARGET}" <<<"${output}"
            grep -Fq "rustc: rustc ${EXPECTED_RUST_RELEASE} " <<<"${output}"
        done
    '

readonly archive_root="beryl-${version}-${target}"
readonly archive_name="${archive_root}.tar.gz"
readonly checksum_name="${archive_name}.sha256"
readonly packages_dir="${build_root}/packages"
mkdir -p "${packages_dir}"
[[ -d ${packages_dir} && ! -L ${packages_dir} ]] \
    || die "packages path is not a directory: ${packages_dir}"

stage_parent=$(mktemp -d "${build_root}/package-stage.XXXXXX")
archive_tmp=$(mktemp "${packages_dir}/.${archive_name}.XXXXXX")
checksum_tmp=$(mktemp "${packages_dir}/.${checksum_name}.XXXXXX")
readonly stage_parent archive_tmp checksum_tmp
cleanup() {
    rm -rf -- "${stage_parent}"
    rm -f -- "${archive_tmp}" "${checksum_tmp}"
}
trap cleanup EXIT

stage_root="${stage_parent}/${archive_root}"
readonly stage_root
install -d -m 0755 \
    "${stage_root}/bin" \
    "${stage_root}/libexec" \
    "${stage_root}/conf" \
    "${stage_root}/systemd" \
    "${stage_root}/logrotate" \
    "${stage_root}/tmpfiles"
install -m 0755 "${artifacts_dir}/beryl" "${stage_root}/bin/beryl"
install -m 0755 "${artifacts_dir}/beryl-metadata" "${stage_root}/libexec/beryl-metadata"
install -m 0755 "${artifacts_dir}/beryl-worker" "${stage_root}/libexec/beryl-worker"
install -m 0644 "${repo_root}/conf/metadata.yaml" "${stage_root}/conf/metadata.yaml"
install -m 0644 "${repo_root}/conf/worker.yaml" "${stage_root}/conf/worker.yaml"
install -m 0644 "${repo_root}/conf/client.yaml" "${stage_root}/conf/client.yaml"
install -m 0644 \
    "${repo_root}/release/systemd/beryl-metadata.service" \
    "${stage_root}/systemd/beryl-metadata.service"
install -m 0644 \
    "${repo_root}/release/systemd/beryl-worker.service" \
    "${stage_root}/systemd/beryl-worker.service"
install -m 0644 "${repo_root}/release/logrotate/beryl" "${stage_root}/logrotate/beryl"
install -m 0644 "${repo_root}/release/tmpfiles/beryl.conf" "${stage_root}/tmpfiles/beryl.conf"
install -m 0755 "${repo_root}/release/install.sh" "${stage_root}/install.sh"
install -m 0644 "${repo_root}/LICENSE" "${stage_root}/LICENSE"
install -m 0644 "${repo_root}/OPERATIONS.md" "${stage_root}/OPERATIONS.md"
install -m 0644 "${repo_root}/README.md" "${stage_root}/README.md"

cat >"${stage_root}/VERSION" <<EOF
manifest_version=1
product=beryl
version=${version}
target=${target}
source_revision=${source_revision}
source_date_epoch=${source_date_epoch}
builder_os=${builder_os}
builder_image_id=${builder_image_id}
base_image=${base_image}
rust_release=${rust_release}
rustup_release=${rustup_release}
protoc_release=${protoc_release}
containerfile_sha256=${containerfile_sha}
repository_definition_sha256=${repository_definition_sha}
cargo_lock_sha256=${cargo_lock_sha}
beryl_sha256=${beryl_sha}
beryl_metadata_sha256=${beryl_metadata_sha}
beryl_worker_sha256=${beryl_worker_sha}
builder_rpms_sha256=${builder_rpms_sha}
EOF
chmod 0644 "${stage_root}/VERSION"

archive_tmp_name=$(basename -- "${archive_tmp}")
readonly archive_tmp_name
podman run --rm \
    --platform linux/amd64 \
    --security-opt label=disable \
    --env "ARCHIVE_ROOT=${archive_root}" \
    --env "ARCHIVE_OUTPUT=${archive_tmp_name}" \
    --env "SOURCE_DATE_EPOCH=${source_date_epoch}" \
    --volume "${stage_parent}:/package:ro" \
    --volume "${packages_dir}:/output:rw" \
    "${builder_image_id}" \
    /bin/bash -Eeuo pipefail -c '
        export LC_ALL=C TZ=UTC
        tar \
            --format=ustar \
            --sort=name \
            --mtime="@${SOURCE_DATE_EPOCH}" \
            --owner=0 \
            --group=0 \
            --numeric-owner \
            -C /package \
            -cf - \
            "${ARCHIVE_ROOT}" \
            | gzip -n -9 >"/output/${ARCHIVE_OUTPUT}"
    '

expected_archive_list=$(cat <<EOF
${archive_root}/
${archive_root}/LICENSE
${archive_root}/OPERATIONS.md
${archive_root}/README.md
${archive_root}/VERSION
${archive_root}/bin/
${archive_root}/bin/beryl
${archive_root}/conf/
${archive_root}/conf/client.yaml
${archive_root}/conf/metadata.yaml
${archive_root}/conf/worker.yaml
${archive_root}/install.sh
${archive_root}/libexec/
${archive_root}/libexec/beryl-metadata
${archive_root}/libexec/beryl-worker
${archive_root}/logrotate/
${archive_root}/logrotate/beryl
${archive_root}/systemd/
${archive_root}/systemd/beryl-metadata.service
${archive_root}/systemd/beryl-worker.service
${archive_root}/tmpfiles/
${archive_root}/tmpfiles/beryl.conf
EOF
)
actual_archive_list=$(
    podman run --rm \
        --platform linux/amd64 \
        --security-opt label=disable \
        --volume "${packages_dir}:/output:ro" \
        "${builder_image_id}" \
        tar -tzf "/output/${archive_tmp_name}"
)
[[ ${actual_archive_list} == "${expected_archive_list}" ]] \
    || die "archive file list does not match the release allowlist"
chmod 0644 "${archive_tmp}"

archive_path="${packages_dir}/${archive_name}"
checksum_path="${packages_dir}/${checksum_name}"
readonly archive_path checksum_path
if [[ -f ${archive_path} ]]; then
    cmp -s "${archive_tmp}" "${archive_path}" \
        || die "existing archive differs for the same build root: ${archive_path}"
    rm -f -- "${archive_tmp}"
else
    mv -- "${archive_tmp}" "${archive_path}"
fi
chmod 0644 "${archive_path}"

archive_sha=$(sha256sum "${archive_path}" | awk '{print $1}')
printf '%s  %s\n' "${archive_sha}" "${archive_name}" >"${checksum_tmp}"
chmod 0644 "${checksum_tmp}"
mv -- "${checksum_tmp}" "${checksum_path}"
(
    cd -- "${packages_dir}"
    sha256sum --check --strict "${checksum_name}"
)

printf 'Release package completed successfully.\n'
printf 'Archive: %s\n' "${archive_path}"
printf 'SHA-256: %s\n' "${archive_sha}"
printf 'Checksum: %s\n' "${checksum_path}"
