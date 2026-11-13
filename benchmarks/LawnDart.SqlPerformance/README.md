# SQL performance harness

This folder loads a fixed data set and times four event store workloads on SQL Server and on Postgres. The short Postgres append counter stays in `LawnDart.Postgres.Benchmarks`.

Timings are printed only when you run the harness yourself. They are written under `results/`, which is gitignored. The `check` command loads data and runs each workload, and it prints no timings.

## Load

```bash
dotnet run --project benchmarks/LawnDart.SqlPerformance -c Release -- load --save
```

The default size is one million events. Pass `--size 100M` for one hundred million. `--size 1B` is accepted for a later machine.

`--database postgres` or `--database sqlserver` runs one database. The default is both, one after the other.

`--seed` and `--tenants` change the generator. The same seed and size produce the same rows.

The one hundred million size raises the database memory cap. Give the container engine about forty gigabytes, and put its data on a large disk.

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

`--from-saved` restores the copy, then times these workloads:

1. Appends. Stream appends at several writer counts, a hot stream, and dynamic consistency boundary appends with conditions.
2. Dynamic consistency boundary queries, plus append condition checks with a recent position and with an empty position.
3. Aggregate stream loads. Each load reads the snapshot and the tail, cold and warm.
4. Projection catch-up, reading the log in two batch sizes, and subscription polling while one writer transaction is open.

`--samples` sets how many operations a case records. Catch-up still reads the loaded log.

The result file is under `results/`. It records the commit, the database, the size, and the timings.

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

Continuous integration runs `check` in its own job. That job does not print timings.

## Databases that are already running

Set `LAWNDART_SQLPERF_PG` or `LAWNDART_SQLPERF_SQL` to a connection string. The harness uses that database and does not start a container.

Set `LAWNDART_SQLPERF_PG_IMAGE` or `LAWNDART_SQLPERF_SQL_IMAGE` to choose the image when the harness starts a container. `LAWNDART_SQLPERF_PG_MEMORY` and `LAWNDART_SQLPERF_SQL_MEMORY` set the container memory cap, for example `4g`.
