# Running a my-open-data store (Docker Compose / Dockge)

One stack holds a whole store: its Postgres catalogue, the Parquet files, the dumps and the
publisher. It runs the same on a laptop, a NAS or a rented server.

1. Copy `.env.example` to `.env` next to `compose.yaml` and fill it (directories, passwords,
   where the catalogue port is published, the my-open-data commit).
2. `docker compose up -d` (in Dockge: Démarrer). `catalog` stays up; `init` creates the roles and
   the database; `install` installs my-open-data; `backup` dumps the catalogue to `backups/`;
   `publish` publishes what is in `landing/`, never without a successful dump.
   `install` and `publish` need outbound internet (pip and git; DuckDB downloads its extensions at
   each start). `docker compose up -d` (Dockge: Démarrer) returns success even if `publish` fails:
   read `docker compose logs publish` (Dockge: the publish container's log) for the JSON report and
   the exit status.
3. Approve each contract once, before its first publish: set `MYOPENDATA_APPROVE` and
   `MYOPENDATA_APPROVED_BY` in `.env`, start again. Approving again is a no-op.
4. To publish again, put new batches in `landing/` and start the stack again.

Producers on the same machine write into `<MYOPENDATA_HOST_DIR>/landing`. Producers elsewhere write
to a local landing and copy it with `push_landing.py SRC DEST` (for example to an SMB mount): one
atomic rename per batch, so the publisher never sees half a batch.

Readers elsewhere connect as `lake_reader` on `MYOPENDATA_PG_BIND` and set `MYOPENDATA_DATA_ROOT`
to where they see the same files; read-only sessions accept a data path that differs from the
publisher's.

Backups: the dump covers the catalogue; dumps accumulate, one per start, so prune `backups/`
yourself; protect the directories with your machine's snapshots. The
store can be rebuilt by publishing `landing/_published/` again. `roles.sql` is the same SQL as
`init`, for a store on a Postgres you already run.
