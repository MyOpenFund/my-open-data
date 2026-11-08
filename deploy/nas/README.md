# Running a my-open-data store (Docker Compose / Dockge)

One stack holds a whole store: its Postgres catalogue, the Parquet files, the dumps and the
publisher. It runs the same on a laptop, a NAS or a rented server.

1. Copy `.env.example` to `.env` next to `compose.yaml` and fill it (the store directory, passwords,
   where the catalogue port is published, the my-open-data commit). Passwords take effect only
   when the catalogue and roles are first created; to change one later, run `ALTER ROLE ...
   PASSWORD` by hand (for the admin, inside Postgres) and update `.env` to match.
   `MYOPENDATA_PG_BIND` must include a private address (`127.0.0.1:5433`, or a VPN address); a
   bare port publishes on all interfaces.
2. `docker compose up -d` (in Dockge: Démarrer). `catalog` stays up; `init` creates the roles and
   the database; `install` installs my-open-data; `backup` dumps the catalogue to `backups/`;
   `publish` publishes what is in `landing/`, never without a successful dump.
   `install` and `publish` need outbound internet (pip and git; DuckDB downloads its extensions at
   each start). `docker compose up -d` (Dockge: Démarrer) returns success even if `publish` fails:
   read `docker compose logs publish` (Dockge: the publish container's log) for the JSON report and
   the exit status. After a failed backup the `publish` container still shows its previous run's
   exit status: check `backup`'s status and the timestamps first.
3. Approve each contract once, before its first publish: set `MYOPENDATA_APPROVE` and
   `MYOPENDATA_APPROVED_BY` in `.env`, start again. Approving again is a no-op. Do it before the
   first batches land (or start the stack once with an empty `landing/` and the approvals set).
   A batch published without an approved contract is quarantined and moved to
   `landing/_quarantine/`; `push_landing.py` will not copy it again, so move it back into
   `landing/<table>/` by hand once the contract is approved.
4. To publish again, put new batches in `landing/` and start the stack again.

Producers on the same machine write into `<MYOPENDATA_HOST_DIR>/landing`. Producers elsewhere write
to a local landing and copy it with `push_landing.py SRC DEST` (for example to an SMB mount): one
atomic rename per batch, so the publisher never sees half a batch.

Readers elsewhere connect as `lake_reader` on `MYOPENDATA_PG_BIND` and set `MYOPENDATA_DATA_ROOT`
to where they see the same files; read-only sessions accept a data path that differs from the
publisher's.

Backups: the catalogue's Postgres data lives in the Docker volume `<project>_pgdata`, outside
`MYOPENDATA_HOST_DIR`, because Postgres must `chmod 0700` its data directory and datasets with
NFSv4 ACLs (typical of SMB shares, e.g. TrueNAS) refuse it; a bind mount to a POSIX directory
would also work. Your machine's snapshots of `MYOPENDATA_HOST_DIR` therefore cover the files and
the dumps, not the live catalogue: the dump in `backups/` is the catalogue's backup. `backup` runs
before `publish`, so the newest dump never holds the latest publish. Dumps accumulate, one per
start; prune `backups/` yourself.

**The volume is the catalogue.** `docker compose down -v`, `docker volume prune`, deleting the
stack in Dockge, or starting it under another project name (Dockge: another stack name) loses
the catalogue or leaves it in a volume the stack no longer uses: the store then starts empty.

To restore a dump into an empty catalogue (the stack must not have published yet, or the
`ducklake_*` tables already exist):

```sh
docker compose up -d --wait catalog
docker compose run --rm init
docker compose run --rm backup sh -c 'pg_restore -d "$PGDATABASE" /data/backups/<file>.dump'
```

then republish the batches of `landing/_published/` newer than the dump (move them back into
`landing/<table>/`), and start the stack. Without any dump, the store can be rebuilt by
publishing all of `landing/_published/` again.

`roles.sql` is the same SQL as `init`, for a store on a Postgres you already run.
