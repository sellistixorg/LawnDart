# SQL performance harness

This folder loads a fixed data set and times four event store workloads on SQL Server and on Postgres. The short Postgres append counter stays in `LawnDart.Postgres.Benchmarks`.

Timings are printed only when you run the harness yourself. They are written under `results/`, which is gitignored. The `check` command loads data and runs each workload. On Postgres it prints whether the cold stream load cleared shared buffers. It prints no timings.

## Load

```bash
dotnet run --project benchmarks/LawnDart.SqlPerformance -c Release -- load --save
```

The default size is one million events. Pass `--size 10M` for ten million, `--size 100M` for one hundred million, or `--size 1B` for one billion. One billion is accepted for a later machine.

`--database postgres` or `--database sqlserver` runs one database. The default is both, one after the other.

`--seed` and `--tenants` change the generator. The same seed and size produce the same rows.

Ten million events on SQL Server use an 8 gigabyte container. One hundred million events, and one billion, raise the cap again. Give the container engine about forty gigabytes for those sizes, and put its data on a large disk.

## Save and restore

`load --save` keeps a copy. Postgres uses a template database. SQL Server uses a backup file. The copy stays in the container volume, so the next run can start from it.

```bash
dotnet run --project benchmarks/LawnDart.SqlPerformance -c Release -- restore --database postgres
dotnet run --project benchmarks/LawnDart.SqlPerformance -c Release -- save --database sqlserver
```

## Run

```bash
dotnet run --project benchmarks/LawnDart.SqlPerformance -c Release -- run --from-saved
```

`--from-saved` restores the copy, then runs the cases below.

`--samples` sets how many operations a case records. Catch-up still reads the loaded log.

The result file is under `results/`. It records the commit, the database, the size, and the timings.

## Cases

Appends:

- `stream writers 1`, `stream writers 8`, `stream writers 32`, and `stream writers 128`. That many streams each append one event per round, at the same time.
- `hot stream`. One stream appends one event after another.
- `dcb append recent after`. One event is appended under a condition. The condition looks for the tag `bench-absent` after the loaded head. That tag is not in the loaded data.

Queries:

- `tag`, `tag and type`, and `two tags`. Each read asks for up to 50 events.
- `condition recent after`. The same `bench-absent` condition, read from the index, starting after the loaded head. This is a read. No event is written.
- `condition empty after`. The same read with no start position. This is a read. No event is written.

`dcb append recent after` writes. `condition recent after` only reads. They are separate cases.

Stream loads. Each load reads the snapshot and the tail.

- `cold`. The buffer cache is cleared before the read. SQL Server runs `CHECKPOINT` and `DBCC DROPCLEANBUFFERS`. Postgres runs `CHECKPOINT` and `pg_buffercache_evict_all` when that function is available. On a container the harness started, when that function is not available, Postgres restarts so shared buffers start empty.
- `connection-cold`. Postgres uses this name when shared buffers cannot be cleared. The harness runs `DISCARD ALL` on its own session. Shared buffers stay, and the timed read uses the store connection.
- `warm`. The same read, with the cache left as it is.

Catch-up:

- `batch 1000` and `batch 5000`. Read the log in pages of that size.
- `subscription poll`. One event is appended while another transaction is open. The subscription then waits until that transaction ends. The recorded time and the throughput are that wait. The open transaction and the append are outside the throughput.

## Compare

```bash
dotnet run --project benchmarks/LawnDart.SqlPerformance -c Release -- compare --left results/left.json --right results/right.json
```

Compare two runs of the same database and the same size. The text is written under `results/` and printed on the machine that ran it.

```bash
dotnet run --project benchmarks/LawnDart.SqlPerformance -c Release -- compare-commits --baseline <commit> --candidate <commit> --database postgres
```

Save a database first. This command restores that copy, builds this harness against each commit's stores, and runs the workloads. The commit has to build with this harness.

## Check

```bash
dotnet run --project benchmarks/LawnDart.SqlPerformance -c Release -- check
```

Continuous integration runs `check --size 1M` in its own job. That job does not print timings.

## Databases that are already running

Set `LAWNDART_SQLPERF_PG` or `LAWNDART_SQLPERF_SQL` to a connection string. The harness uses that database and does not start a container.

Set `LAWNDART_SQLPERF_PG_IMAGE` or `LAWNDART_SQLPERF_SQL_IMAGE` to choose the image when the harness starts a container. `LAWNDART_SQLPERF_PG_MEMORY` and `LAWNDART_SQLPERF_SQL_MEMORY` set the container memory cap, for example `4g`.
