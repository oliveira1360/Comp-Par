# ComPar 2026/27, Project 1: large-scale analytics with Spark (Kotlin)

A Kotlin and Apache Spark pipeline over the NYC TLC Yellow Taxi trip records (2015). The project is about performance
engineering: each operation has several versions, and each version is measured against the previous one.
The report is `docs/report.tex` (PDF in `docs/report.pdf`).

## Requirements

- Docker (Docker Desktop on Windows/macOS, Docker Engine on Linux).
- JDK 17 for Gradle (the `./gradlew` wrapper handles the rest).
- Bash. On Windows, Git Bash.
- About 2.5 GB of free disk for the 2015 data.

## Layout

| Path | Contents |
|---|---|
| `src/main/kotlin/compar/Main.kt` | Entry point (`compar.MainKt`) and the version registry of each operation |
| `src/main/kotlin/compar/config/CliArgs.kt` | Command-line options |
| `src/main/kotlin/compar/runner/PipelineRunner.kt` | Runs the selected operations and collects their metrics |
| `src/main/kotlin/compar/ops/` | Operations. `Contracts.kt` defines `DataLoader` (M1) and `Analysis` (M2 to O4); each operation has its own package (`m1/`, `m2/`, `m3/`, `o1/`, `o4/`) with one class per version |
| `src/main/kotlin/compar/profiler/` | Profiler: reads the Spark REST API (stages, tasks, CPU, GC, shuffle, spill, memory) and exports JSON |
| `src/main/kotlin/compar/util/SparkUtils.kt` | `SparkSession` creation and Spark configuration |
| `scripts/` | Cluster (`start_cluster.sh`, `status_cluster.sh`, `stop_cluster.sh`), data (`download_data.sh`), job submission (`submit_job.sh`) and benchmarks (`benchmark.sh`, `cluster_bench/scaling_2015.sh`) |
| `docker/` | `docker-compose.yml` (master, workers and JupyterLab) and the `Dockerfile` of the Jupyter image |
| `notebooks/benchmarks_report.ipynb` | Tables and charts of the report, built from the metrics |
| `notebooks/exploration.ipynb` | Data exploration |
| `docs/` | Report, figures, bibliography and the physical plans behind Figure 1 (`docs/plans/`) |

`data/` and `output/` are not tracked by git.

Each operation has a baseline `v0` and numbered versions, where version N starts from N-1. The KDoc of each class states
the bottleneck and the change. Available versions: M1 v0–v3, M2 v0–v7, M3 v0–v3, O1 v0–v7, O4 v0–v6.
The best ones, used in the report: M1 v3, M2 v4, M3 v3, O1 v6, O4 v6.

## Data

The report uses the 12 months of 2015 (2.0 GB, 146,039,231 trips) and the taxi zone lookup table. To download them into
`data/`:

```bash
./scripts/download_data.sh 2015 1 12
```

The script also downloads `taxi_zone_lookup.csv` and skips files that already exist. The files come from the TLC CDN
(`https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2015-MM.parquet`). Version used in the report:
downloaded in November 2026; the 12 files total 2,027,058,112 bytes (`yellow_tripdata_2015-01.parquet`:
175,325,767 bytes). Official page: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Build and run

```bash
./gradlew jar                                   # builds build/libs/nyc-taxi-analytics.jar (includes kotlin-stdlib)
./scripts/start_cluster.sh                      # 2 workers, 2 GB and 2 cores each
./scripts/submit_job.sh --data "/data/yellow_tripdata_2015-*.parquet" \
  --ops M1,M2,M3,O1,O4 --m1 v3 --m2 v4 --m3 v3 --o1 v6 --o4 v6 --profile
./scripts/stop_cluster.sh
```

`start_cluster.sh [workers] [memory] [cores]` starts a different cluster size, for example `./scripts/start_cluster.sh 3 2G 2`.
`submit_job.sh` builds the jar and submits it to `spark-master` (`--skip-build` skips the build). `status_cluster.sh`
lists the containers.

It also runs outside Docker, in local mode:

```bash
./gradlew run --args="--master local[*] --sample 0.1 --profile"
```

Pipeline options (full list with `--help`):

| Option | Meaning |
|---|---|
| `--data <path>` | Parquet file(s); globs are accepted. Default `data/yellow_tripdata_2015-01.parquet` |
| `--lookup <csv>` | Taxi zone lookup table |
| `--output <dir>` | Output directory |
| `--master <url>` | For example `local[*]` or `spark://spark-master:7077` |
| `--sample <fraction>` | Sample of the trips, for example `0.1` |
| `--ops M1,M2,...` | Operations to run |
| `--m1`, `--m2`, `--m3`, `--o1`, `--o4` `vN` | Version of each operation |
| `--profile` | Enables the profiler |
| `--export-metrics <json>` | Output file for the profiler metrics |

Web UIs while the cluster is up: Spark Master at http://localhost:8080, JupyterLab at
http://localhost:8888/?token=compar, and the application UI at http://localhost:4040 while a job started from Jupyter runs.

## Benchmarks

`benchmark.sh` builds the jar once and runs each version as a separate `spark-submit` on the cluster. Examples:

```bash
./scripts/benchmark.sh --m2 all                 # every M2 version, on the 2015 data
./scripts/benchmark.sh --o1 v5,v6,v7 --runs 3   # three O1 versions, 3 runs each
./scripts/benchmark.sh --all --runs 3           # every version of every operation
```

Other options: `--sample`, `--data`, `--ops` and `--cooldown <s>` (pause between operations, 20 s by default).
Results go to `output/outputs/<op>/<version>/` and metrics to `output/metrics/<op>/profiling_<version>.json`
(with `--runs N`, `profiling_<version>_r<N>.json`). The notebook `notebooks/benchmarks_report.ipynb` reads these metrics
and saves the charts to `output/plots/`.

## Reproducing the report

The runs were made on two machines:

- Windows (main numbers of the report): Docker Desktop on Windows 11, Intel Core Ultra 7 256V (4 P-cores +
  4 E-cores), 16 GB RAM, NVMe SSD, Spark 4.2.0.
- Linux ("Linux" column of Table 2, Figure 4 and Appendix A): Docker Engine without a VM on Fedora Linux 45, Intel Core
  Ultra 7 155H (6 P-cores + 8 E-cores + 2 LP-cores), 32 GB RAM, NVMe SSD, Spark 4.2.1.

On both: image `spark:python3` (Scala 2.13, OpenJDK 21), 1 master and 2 workers with 2 cores and 2 GB each, each container
limited to 2 CPUs (`./scripts/start_cluster.sh 2 2G 2`), driver in the master container (client mode). The jar is built
with JDK 17 and Kotlin 2.0.21 against the Spark 3.5.3 API.

Non-default Spark configuration: `spark.executor.memory=2g` (default 1g), set in
`src/main/kotlin/compar/util/SparkUtils.kt`. The same file sets `spark.driver.memory=2g`, which has no effect in client
mode, and `spark.sql.adaptive.enabled=true`, which is already the default.

Each benchmark run is this `spark-submit`, inside the `spark-master` container:

```bash
docker exec spark-master /opt/spark/bin/spark-submit --class compar.MainKt \
  --master spark://spark-master:7077 --deploy-mode client /apps/nyc-taxi-analytics.jar \
  --data "/data/yellow_tripdata_2015-*.parquet" --lookup /data/taxi_zone_lookup.csv \
  --output /output/outputs/<op>/<version> --export-metrics /output/metrics/<op>/profiling_<version>.json \
  --profile --ops <OP> --m1 v3 --<op> <version>
```

Steps:

```bash
./scripts/download_data.sh 2015 1 12
./scripts/start_cluster.sh 2
./scripts/benchmark.sh --all --runs 3           # Sections 3 and 4 (about 1h30)
./scripts/cluster_bench/scaling_2015.sh         # Section 5: 100/20/10 % of the data, 1/2/3 workers (about 40 min)
```

The report uses the median of the 3 runs. The analytical operations run after M1 v3 and count only their own jobs.
`scaling_2015.sh` runs the optimised pipeline with M1 v2, the version chosen before the repeated runs showed that v3 is
faster, and stores the metrics in `output/scaling/<condition>/`. The tables and charts come from
`notebooks/benchmarks_report.ipynb` (open it in JupyterLab and run all cells).

To build the report, inside `docs/`:

```bash
latexmk -pdf report.tex
```

## Adding a version

Copy the latest version of the operation to a new class (for example `ops/o1/O1V8.kt`), describe the bottleneck and the
change in its KDoc, and register it in the operation's `when` in `Main.kt`.
