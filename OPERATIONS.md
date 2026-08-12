# Beryl Operations Manual

This manual covers the packaged single-Metadata, single-Worker internal alpha
on a systemd Linux host. It does not define mixed-version upgrades, Metadata
HA, replication, or online backup and restore.

## Installed layout

| Path | Purpose | Ownership and mode |
| --- | --- | --- |
| `/opt/beryl` | Immutable package payload and `VERSION` | `root:root`, package modes |
| `/etc/beryl` | Active Metadata, Worker, and Client configuration | directory `0750 root:beryl`; files `0640 root:beryl` |
| `/var/lib/beryl` | Metadata and Worker persistent state | `0750 beryl:beryl` |
| `/var/log/beryl` | Application logs | directory `0750`; files `0640 beryl:beryl` |
| `/etc/systemd/system` | Metadata and Worker units | `0644 root:root` |
| `/etc/logrotate.d/beryl` | Application log retention | `0644 root:root` |
| `/etc/tmpfiles.d/beryl.conf` | Runtime directory and log ownership contract | `0644 root:root` |

The default profile binds RPC and HTTP listeners to `0.0.0.0`. Beryl does not
currently provide transport authentication or TLS. Restrict these ports to a
trusted network with the host firewall and do not expose them to an untrusted
network.

## Clean installation

Verify the archive before extraction:

```bash
sha256sum --check --strict \
  beryl-0.1.0-alpha.1-x86_64-unknown-linux-gnu.tar.gz.sha256
tar -xzf beryl-0.1.0-alpha.1-x86_64-unknown-linux-gnu.tar.gz
cd beryl-0.1.0-alpha.1-x86_64-unknown-linux-gnu
sudo ./install.sh
```

The installer refuses an existing installation or active Beryl service. It
validates the binary hashes recorded in `VERSION`, creates the `beryl` system
identity, installs configuration and service files, applies the tmpfiles
contract, reloads systemd, and validates both configurations. It does not
format storage or start services.

Format Metadata exactly once on a new deployment:

```bash
sudo -u beryl sh -c '
  cd /var/lib/beryl
  umask 027
  /opt/beryl/bin/beryl --conf-dir /etc/beryl format metadata
'
```

Never run `format metadata` against an initialized deployment. Formatting is
not an upgrade or repair operation.

## Start, stop, and restart

Start Metadata first and wait for readiness before starting Worker:

```bash
sudo systemctl enable --now beryl-metadata.service
curl --fail http://127.0.0.1:18081/ready

sudo systemctl enable --now beryl-worker.service
curl --fail http://127.0.0.1:19091/ready
```

Stop data admission first by stopping Worker, then stop Metadata:

```bash
sudo systemctl stop beryl-worker.service
sudo systemctl stop beryl-metadata.service
```

Start an existing deployment in the normal order:

```bash
sudo systemctl start beryl-metadata.service
curl --fail http://127.0.0.1:18081/ready
sudo systemctl start beryl-worker.service
curl --fail http://127.0.0.1:19091/ready
```

Restart one role at a time and wait for readiness after each restart:

```bash
sudo systemctl restart beryl-metadata.service
curl --fail http://127.0.0.1:18081/ready
curl --fail http://127.0.0.1:19091/ready

sudo systemctl restart beryl-worker.service
curl --fail http://127.0.0.1:19091/ready
```

The units send `SIGTERM`, allow up to 60 seconds for bounded shutdown, and only
then permit systemd to send `SIGKILL`. Do not use `kill -9` for routine stops.

## Service and process status

```bash
systemctl status beryl-metadata.service beryl-worker.service
systemctl is-enabled beryl-metadata.service beryl-worker.service
systemctl is-active beryl-metadata.service beryl-worker.service

systemctl show beryl-metadata.service \
  -p MainPID -p ActiveState -p SubState -p Result -p ExecMainStatus
systemctl show beryl-worker.service \
  -p MainPID -p ActiveState -p SubState -p Result -p ExecMainStatus

ss -ltnp | grep -E ':(18080|18081|19090|19091)[[:space:]]'
```

Default endpoints:

| Role | RPC | HTTP |
| --- | --- | --- |
| Metadata | `18080` | `18081` |
| Worker | `19090` | `19091` |

`/health` reports that the process is alive. `/ready` reports whether the role
may currently serve its contract. Use readiness, not health, for admission.

```bash
curl --fail http://127.0.0.1:18081/health
curl --fail http://127.0.0.1:18081/ready
curl --fail http://127.0.0.1:19091/health
curl --fail http://127.0.0.1:19091/ready
```

## Configuration maintenance

Edit only the active files under `/etc/beryl`. Keep ownership
`root:beryl` and mode `0640`. Beryl does not support configuration reload in
place; validate first, then restart the affected role.

```bash
sudo /opt/beryl/bin/beryl --conf-dir /etc/beryl validate-conf
sudo /opt/beryl/bin/beryl --conf-dir /etc/beryl validate-conf metadata
sudo /opt/beryl/bin/beryl --conf-dir /etc/beryl validate-conf worker
```

If both role configurations change, stop Worker, restart Metadata and wait for
Metadata readiness, then start Worker and wait for Worker readiness.

Metadata RPC concurrency is configured with three restart-only values:

- `beryl.metadata.rpc.max-concurrent-requests` bounds all active Metadata RPCs.
- `beryl.metadata.rpc.max-concurrent-requests-per-connection` prevents one
  HTTP/2 connection from consuming the service-wide limit.
- `beryl.metadata.rpc.reserved-control-requests` keeps part of the service-wide
  capacity unavailable to filesystem RPCs so Worker and gRPC health traffic
  can still make progress.

Requests beyond either boundary are rejected immediately with gRPC
`ResourceExhausted`; they are not queued or passed to a Metadata handler. Keep
the per-connection value at or below the service-wide value and the control
reserve below the service-wide value. The shipped defaults are `64`, `16`, and
`8`, respectively.

Write-session limits are also restart-only:

- `beryl.metadata.write-session.max-active` bounds pending plus installed
  write sessions across the Metadata leader.
- `beryl.metadata.write-session.max-active-per-client` bounds the same states
  attributed to one client ID. Client IDs are fairness keys, not authenticated
  tenant identities.

Excess `OpenWrite` calls fail before local lease acquisition or a Raft proposal.
The shipped defaults are `1024` globally and `64` per client.

The current alpha supports clean installation and same-version restart only.
Do not perform an in-place upgrade, downgrade, mixed-version deployment, or
rollback with these procedures.

## Logs

Application logs are separate files:

```bash
tail -F /var/log/beryl/metadata.log
tail -F /var/log/beryl/worker.log
```

systemd lifecycle and unit failures remain in journald:

```bash
journalctl -u beryl-metadata.service --since today
journalctl -u beryl-worker.service --since today
journalctl -u beryl-metadata.service -u beryl-worker.service -f
```

The packaged logrotate policy rotates daily or at 100 MiB, retains 14 files,
compresses old files, and uses `copytruncate` because the processes do not yet
provide a log-reopen signal.

```bash
sudo logrotate --debug /etc/logrotate.d/beryl
sudo logrotate --force --verbose /etc/logrotate.d/beryl
```

Use forced rotation only for validation or an explicit maintenance action.
The installer must create both active log files as `0640 beryl:beryl` before
the first service start; `/etc/tmpfiles.d/beryl.conf` enforces this contract.

## Metrics

Both HTTP endpoints expose Prometheus text format:

```bash
curl --fail http://127.0.0.1:18081/metrics
curl --fail http://127.0.0.1:19091/metrics
```

Useful Metadata signals:

- `metadata_up`, `metadata_root_ready`, and `metadata_raft_role`
- `metadata_raft_term`, `metadata_raft_last_applied_index`, and
  `metadata_raft_committed_index`
- `metadata_worker_live`, worker registration, heartbeat, and block-report
  counters
- filesystem and RPC request totals and duration histograms
- `grpc_server_requests_inflight` and
  `grpc_server_concurrency_rejections_total`, labeled by traffic class and limit
  scope
- cleanup candidates, commands, retries, anomalies, and reclaim progress

Useful Worker signals:

- `worker_up` and `worker_registered`
- store capacity, writable state, and block count by directory
- Metadata RPC, heartbeat, and block-report counters
- data RPC, stream, frame, commit, and abort metrics
- cleanup queue depth, reclaiming count, and cleanup results

Compact checks:

```bash
curl --silent http://127.0.0.1:18081/metrics \
  | grep -E '^(metadata_up|metadata_root_ready|metadata_raft_role|metadata_worker_live)'
curl --silent http://127.0.0.1:19091/metrics \
  | grep -E '^(worker_up|worker_registered|worker_store_(capacity_bytes|writable|blocks))'
```

Example Prometheus targets:

```yaml
scrape_configs:
  - job_name: beryl-metadata
    static_configs:
      - targets: ["127.0.0.1:18081"]
  - job_name: beryl-worker
    static_configs:
      - targets: ["127.0.0.1:19091"]
```

## Storage and cleanup maintenance

Check capacity and file ownership without modifying state:

```bash
du -sh /var/lib/beryl/data/metadata /var/lib/beryl/data/worker
find /var/lib/beryl -xdev -not -user beryl -print
curl --silent http://127.0.0.1:19091/metrics \
  | grep -E '^worker_store_(capacity_bytes|writable|blocks)'
```

Namespace deletion is immediate from the client perspective. Physical Worker
blocks are reclaimed asynchronously after the configured Metadata cleanup
grace period, scan interval, heartbeat delivery, and Worker execution. Monitor
the Metadata and Worker cleanup metrics instead of deleting block files by
hand.

Do not copy live RocksDB or Worker block directories as an online backup. The
current alpha has no supported online backup, restore, or cross-version data
migration contract. Preserve state only during a controlled same-version
offline investigation, with both services stopped.

## Initial diagnostics

If a unit fails or never becomes ready:

1. Run `beryl --conf-dir /etc/beryl validate-conf`.
2. Inspect `systemctl status` and `journalctl -u` for lifecycle failures.
3. Inspect the role's application log under `/var/log/beryl`.
4. Check listener conflicts with `ss -ltnp`.
5. Check ownership and capacity under `/var/lib/beryl` and `/var/log/beryl`.
6. Check Metadata and Worker `/metrics` for readiness, registration, storage,
   RPC, and cleanup signals.

Never repair Metadata or Worker state by deleting individual files. Stop and
preserve the exact state for diagnosis when a persistent-format or corruption
check fails.
