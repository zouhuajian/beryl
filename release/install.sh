#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: 2026 Beryl Contributors

set -Eeuo pipefail
IFS=$'\n\t'
umask 022

die() {
    printf 'error: %s\n' "$*" >&2
    exit 1
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

usage() {
    cat <<EOF
Usage: $(basename -- "$0")

Install this Beryl package on a clean systemd host. The installer validates
the package, creates the service identity and runtime paths, installs static
configuration, and reloads systemd. It does not format storage or start units.
EOF
}

case ${1:-} in
    "")
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
[[ $# -eq 0 ]] || die "this installer takes no arguments"

[[ ${EUID} -eq 0 ]] || die "run this installer as root"
[[ $(uname -s) == "Linux" ]] || die "Beryl can only be installed on Linux"
[[ $(uname -m) == "x86_64" ]] || die "this package requires x86_64"
[[ -d /run/systemd/system ]] || die "systemd is not running"

for command in \
    awk \
    basename \
    cat \
    chmod \
    chown \
    cp \
    dirname \
    getent \
    groupadd \
    id \
    install \
    mktemp \
    mv \
    rm \
    runuser \
    sh \
    sha256sum \
    stat \
    systemctl \
    systemd-analyze \
    systemd-tmpfiles \
    uname \
    useradd; do
    require_command "${command}"
done

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
readonly script_dir
readonly manifest="${script_dir}/VERSION"

manifest_value() {
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
    ' "${manifest}"
}

[[ -f ${manifest} && ! -L ${manifest} ]] || die "VERSION is not a regular file"
manifest_version=$(manifest_value manifest_version) || die "VERSION has no unique manifest_version"
product=$(manifest_value product) || die "VERSION has no unique product"
version=$(manifest_value version) || die "VERSION has no unique version"
release_tag=$(manifest_value release_tag) || die "VERSION has no unique release_tag"
target=$(manifest_value target) || die "VERSION has no unique target"
source_revision=$(manifest_value source_revision) || die "VERSION has no unique source_revision"
beryl_sha=$(manifest_value beryl_sha256) || die "VERSION has no unique beryl_sha256"
metadata_sha=$(manifest_value beryl_metadata_sha256) || die "VERSION has no unique beryl_metadata_sha256"
worker_sha=$(manifest_value beryl_worker_sha256) || die "VERSION has no unique beryl_worker_sha256"
readonly manifest_version product version release_tag target source_revision
readonly beryl_sha metadata_sha worker_sha

[[ ${manifest_version} == "1" ]] || die "unsupported manifest version: ${manifest_version}"
[[ ${product} == "beryl" ]] || die "package product is not beryl: ${product}"
[[ ${target} == "x86_64-unknown-linux-gnu" ]] || die "unsupported package target: ${target}"
[[ ${version} =~ ^[0-9A-Za-z][0-9A-Za-z.+-]*$ ]] || die "invalid package version: ${version}"
[[ ${release_tag} == "unreleased" || ${release_tag} == "v${version}" ]] \
    || die "release tag does not match package version: ${release_tag}"
[[ ${source_revision} =~ ^[0-9a-f]{40}$|^[0-9a-f]{64}$ ]] || die "invalid source revision"
for digest in "${beryl_sha}" "${metadata_sha}" "${worker_sha}"; do
    [[ ${digest} =~ ^[0-9a-f]{64}$ ]] || die "VERSION contains an invalid binary SHA-256"
done

for package_input in \
    "${script_dir}/bin/beryl" \
    "${script_dir}/libexec/beryl-metadata" \
    "${script_dir}/libexec/beryl-worker" \
    "${script_dir}/conf/client.yaml" \
    "${script_dir}/conf/metadata.yaml" \
    "${script_dir}/conf/worker.yaml" \
    "${script_dir}/systemd/beryl-metadata.service" \
    "${script_dir}/systemd/beryl-worker.service" \
    "${script_dir}/logrotate/beryl" \
    "${script_dir}/tmpfiles/beryl.conf" \
    "${script_dir}/OPERATIONS.md"; do
    [[ -f ${package_input} && ! -L ${package_input} ]] \
        || die "package input is not a regular file: ${package_input}"
done
[[ -x ${script_dir}/bin/beryl ]] || die "beryl is not executable"
[[ -x ${script_dir}/libexec/beryl-metadata ]] || die "beryl-metadata is not executable"
[[ -x ${script_dir}/libexec/beryl-worker ]] || die "beryl-worker is not executable"
[[ $(sha256sum "${script_dir}/bin/beryl" | awk '{print $1}') == "${beryl_sha}" ]] \
    || die "beryl does not match VERSION"
[[ $(sha256sum "${script_dir}/libexec/beryl-metadata" | awk '{print $1}') == "${metadata_sha}" ]] \
    || die "beryl-metadata does not match VERSION"
[[ $(sha256sum "${script_dir}/libexec/beryl-worker" | awk '{print $1}') == "${worker_sha}" ]] \
    || die "beryl-worker does not match VERSION"

for service in beryl-metadata.service beryl-worker.service; do
    if systemctl is-active --quiet "${service}"; then
        die "service is already active: ${service}"
    fi
done

for path in \
    /opt/beryl \
    /etc/beryl \
    /var/lib/beryl \
    /var/log/beryl \
    /etc/systemd/system/beryl-metadata.service \
    /etc/systemd/system/beryl-worker.service \
    /etc/logrotate.d/beryl \
    /etc/tmpfiles.d/beryl.conf; do
    [[ ! -e ${path} && ! -L ${path} ]] || die "clean install target already exists: ${path}"
done

if getent group beryl >/dev/null; then
    [[ $(getent group beryl | awk -F: '{print $1}') == "beryl" ]] \
        || die "cannot resolve the existing beryl group"
else
    groupadd --system beryl
fi

if id beryl >/dev/null 2>&1; then
    [[ $(id -gn beryl) == "beryl" ]] || die "existing beryl user has the wrong primary group"
    [[ $(getent passwd beryl | awk -F: '{print $6}') == "/var/lib/beryl" ]] \
        || die "existing beryl user has the wrong home directory"
else
    useradd \
        --system \
        --gid beryl \
        --home-dir /var/lib/beryl \
        --shell /sbin/nologin \
        --no-create-home \
        beryl
fi

install_stage=$(mktemp -d /opt/.beryl-install.XXXXXX)
readonly install_stage
installed=0
cleanup() {
    if [[ ${installed} -eq 0 ]]; then
        rm -rf -- "${install_stage}"
    fi
}
trap cleanup EXIT

cp -a -- "${script_dir}/." "${install_stage}/"
chown -R root:root "${install_stage}"
chmod 0755 "${install_stage}"
mv -- "${install_stage}" /opt/beryl
installed=1

install -d -m 0750 -o root -g beryl /etc/beryl
install -m 0640 -o root -g beryl /opt/beryl/conf/client.yaml /etc/beryl/client.yaml
install -m 0640 -o root -g beryl /opt/beryl/conf/metadata.yaml /etc/beryl/metadata.yaml
install -m 0640 -o root -g beryl /opt/beryl/conf/worker.yaml /etc/beryl/worker.yaml
install -m 0644 -o root -g root \
    /opt/beryl/systemd/beryl-metadata.service \
    /etc/systemd/system/beryl-metadata.service
install -m 0644 -o root -g root \
    /opt/beryl/systemd/beryl-worker.service \
    /etc/systemd/system/beryl-worker.service
install -m 0644 -o root -g root /opt/beryl/logrotate/beryl /etc/logrotate.d/beryl
install -m 0644 -o root -g root /opt/beryl/tmpfiles/beryl.conf /etc/tmpfiles.d/beryl.conf

systemd-tmpfiles --create /etc/tmpfiles.d/beryl.conf
for log_file in /var/log/beryl/metadata.log /var/log/beryl/worker.log; do
    [[ $(stat -c '%a %U:%G' "${log_file}") == "640 beryl:beryl" ]] \
        || die "tmpfiles created an invalid log file: ${log_file}"
done

systemctl daemon-reload
systemd-analyze verify \
    /etc/systemd/system/beryl-metadata.service \
    /etc/systemd/system/beryl-worker.service
runuser -u beryl -- sh -c \
    'cd /var/lib/beryl && /opt/beryl/bin/beryl --conf-dir /etc/beryl validate-conf'

printf 'Beryl %s installed successfully from %s.\n' "${version}" "${source_revision}"
printf 'Review /opt/beryl/OPERATIONS.md before formatting storage or starting services.\n'
