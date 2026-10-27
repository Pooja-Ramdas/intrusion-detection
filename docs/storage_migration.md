# Current status (2026-10-27)

The migration described below has been executed and verified; the original plan text is retained as the procedure record and is superseded by these measured results.

- `scripts/migrate_volumes_to_d.ps1 -Apply` copied the existing NameNode, DataNode, and MongoDB volumes to `D:\bda`, then started the existing Compose services with `docker-compose.storage-d.yml`. No named volume was deleted or pruned.
- Active bind mounts: `D:\bda\hdfs\nn`, `dn1`, `dn2`, `mongo\data`, `mongo\configdb`, and `spark-tmp`; app staging/models/logs are also under `D:\bda`.
- The old named volumes remain as unlinked rollback copies. Docker reported 2.613 GB for all five inactive local volumes. Do not prune them.
- Stable NameNode VERSION/fsimage files: 5 files / 15,917 bytes, SHA-256 matched. DataNode block and metadata files: DN1 134 files / 1,423,962,378 bytes; DN2 62 files / 866,652,438 bytes; exact SHA-256 matched. Mongo configdb was empty in both copies. Mongo’s live WiredTiger files were not hash-compared because they changed after startup; Mongo ping, database names, and service health were checked instead.
- NameNode edit logs advanced after startup; the original named volume is a frozen rollback snapshot, not a live mirror.
- Compose config validation passed. All eight services were healthy at the last check. `hdfs fsck /bda` reported HEALTHY, 0 under-replicated, 0 missing, and 0 corrupt blocks (see `docs/phase_1_report.md`).
- Docker Desktop’s disk image remains at `D:\DockerDesktopWSL\disk\docker_data.vhdx`; no Docker image move was performed.

## Original migration procedure (record)

## Current state

- Docker Desktop WSL storage is on D: at `D:\DockerDesktopWSL\disk\docker_data.vhdx` (18,382,585,856 bytes at audit time).
- The WSL distro base is `D:\DockerDesktopWSL\main`; its `ext4.vhdx` is 109,051,904 bytes. `wsl --list --verbose` showed only `docker-desktop` (WSL 2).
- Current persistent mounts are named volumes `bda_namenode_data`, `bda_datanode1_data`, `bda_datanode2_data`, and `bda_mongo_data`. They are healthy, populated, and preserved by the base Compose file.
- The base Compose file deliberately continues to use these existing named volumes. `docker-compose.storage-d.yml` is an opt-in bind-mount override and must not be applied until this procedure completes.

## Copy and verify procedure

1. Confirm `D:\bda` has at least 12 GiB free using `scripts\disk_guard.ps1`. Do not proceed if it exits nonzero. Record the output and current `docker compose ps --all`.
2. Create `D:\bda\hdfs\nn`, `D:\bda\hdfs\dn1`, `D:\bda\hdfs\dn2`, `D:\bda\mongo`, `D:\bda\mongo\configdb`, `D:\bda\spark-tmp`, `D:\bda\staging`, `D:\bda\models`, and `D:\bda\logs`. Do not remove or rename any current volume.
3. Stop (do not remove) the services with `docker compose stop`. This cleanly stops MongoDB and HDFS before copying their live data directories.
4. The current Mongo `/data/configdb` source is anonymous volume `9d7bf3343d08b462a3d025ef7cab7137632d16935dc34fbcb4844b11f1cff9c7` (Docker reports 0 B). Copy NN, DN1, DN2, Mongo DB, and configdb separately using the read-only source and exact D: destinations. Example for the NameNode:

   ```powershell
   docker run --rm --user 0:0 --entrypoint /bin/bash `
     --mount type=volume,source=bda_namenode_data,target=/src,readonly `
     --mount type=bind,source=D:/bda/hdfs/nn,target=/dst `
     bda-spark:latest -lc 'tar -cpf - -C /src . | tar -xpf - -C /dst'
   ```

   Use source volumes `bda_datanode1_data`, `bda_datanode2_data`, and `bda_mongo_data` with destinations `D:/bda/hdfs/dn1`, `D:/bda/hdfs/dn2`, and `D:/bda/mongo/data`. Use the anonymous configdb volume above with destination `D:/bda/mongo/configdb`.
5. After each copy, use the same mounts to generate sorted SHA-256 manifests and compare them:

   ```powershell
   docker run --rm --user 0:0 --entrypoint /bin/bash `
     --mount type=volume,source=bda_namenode_data,target=/src,readonly `
     --mount type=bind,source=D:/bda/hdfs/nn,target=/dst `
     bda-spark:latest -lc '(cd /src && find . -type f -print0 | sort -z | xargs -0 sha256sum) >/tmp/src.sha; (cd /dst && find . -type f -print0 | sort -z | xargs -0 sha256sum) >/tmp/dst.sha; diff -u /tmp/src.sha /tmp/dst.sha'
   ```

   Repeat with all other volume/destination pairs. A zero exit code from `diff` is required. Confirm file counts and byte totals match. Repeat `scripts\disk_guard.ps1` before each volume copy and verification pass.
6. Keep the named source volumes unchanged. Start with `docker compose -f docker-compose.yml -f docker-compose.storage-d.yml up -d` only after all copy manifests match. Verify every service health state, `hdfs fsck /bda`, HDFS file counts and sizes, MongoDB ping and database stats. Compare the resulting HDFS/Mongo measurements with the audit baseline.
7. If verification fails, stop the bind-mounted services and restart with the base file only. The original named volumes remain the rollback copy. Do not delete them during this task.

The migration has not run. It requires a maintenance window and a later measured verification. Empty D: target directories and the metadata-only ingest plan exist; no named-volume data has been copied. The VHDX itself is already on D:; do not move it.
