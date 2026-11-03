#!/usr/bin/env bash
# Benchmark on the Docker Spark standalone cluster (docker/docker-compose.yml), ROUNDS times per config,
# rotating the order every round. Each run is a fresh spark-submit (client mode, driver in spark-master).
#   2 workers:   ALL_v0, ALL_best, ALL_best --sample 0.1, ALL_best --sample 0.2
#   1, 3 workers: ALL_best
# Workers have 2 cores and 2 GB each (the code asks for 2 GB executors). Data: data/bench (12 months of 2024).
# usage: scripts/cluster_bench/cluster_matrix.sh     -> output/cluster_bench/{metrics,logs}, progress.txt, env.txt
# then:  visualize in notebooks/benchmarks_report.ipynb
export MSYS_NO_PATHCONV=1
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT" || exit 1
OUT="$ROOT/output/cluster_bench"
ROUNDS=${ROUNDS:-3}
WORKER_COUNTS=${WORKER_COUNTS:-"2 1 3"}
BEST="--ops M1,M2,M3,O1,O4 --m1 v3 --m2 v7 --m3 v3 --o1 v7 --o4 v6"
BASE="--ops M1,M2,M3,O1,O4 --m1 v0 --m2 v0 --m3 v0 --o1 v0 --o4 v0"
mkdir -p "$OUT/metrics" "$OUT/logs"

if [ ! -d data/bench ]; then
    mkdir -p data/bench
    for f in data/yellow_tripdata_*.parquet; do ln "$f" data/bench/ 2>/dev/null || cp "$f" data/bench/; done
fi
./gradlew -q jar || exit 1

start_cluster() {
    bash scripts/stop_cluster.sh > /dev/null 2>&1
    bash scripts/start_cluster.sh "$1" 2G 2 > /dev/null 2>&1
    docker stop jupyter > /dev/null 2>&1   # not needed for the benchmark; frees memory in the Docker VM
    for _ in $(seq 1 60); do
        alive=$(curl -s localhost:8080/json/ | grep -o '"state" *: *"ALIVE"' | wc -l)
        [ "$alive" -ge "$1" ] && return 0
        sleep 2
    done
    echo "workers did not register" >&2; return 1
}

run() {  # run <id> <args...>
    local id=$1; shift
    rm -rf "output/cluster_bench_out/$id"
    docker exec spark-master /opt/spark/bin/spark-submit --class compar.MainKt --master spark://spark-master:7077 \
        --deploy-mode client --driver-memory 2g /apps/nyc-taxi-analytics.jar \
        --data /data/bench --lookup /data/taxi_zone_lookup.csv --output "/output/cluster_bench_out/$id" \
        --profile --export-metrics "/output/cluster_bench/metrics/$id.json" "$@" > "$OUT/logs/$id.log" 2>&1
    echo $?
}

{
    echo "date: $(date -Iseconds)"
    echo "commit: $(git rev-parse --short HEAD) $(git status --porcelain | grep -q . && echo '(dirty)')"
    docker exec spark-master /opt/spark/bin/spark-submit --version 2>&1 | grep -E "version [0-9]|Scala"
    docker info --format 'docker VM: {{.NCPU}} CPUs, {{.MemTotal}} bytes, {{.OperatingSystem}}'
    echo "workers: ${WORKER_COUNTS} x (2 cores, 2G); executor 2g; driver 2g in spark-master (client mode)"
} > "$OUT/env.txt"

: > "$OUT/progress.txt"
for w in $WORKER_COUNTS; do
    start_cluster "$w" || exit 1
    configs=("ALL-w${w}_best|$BEST")
    if [ "$w" = 2 ]; then
        configs+=("ALL-w2_v0|$BASE" "ALL-w2f10_best|$BEST --sample 0.1" "ALL-w2f20_best|$BEST --sample 0.2")
    fi
    n=${#configs[@]}
    for r in $(seq 1 "$ROUNDS"); do
        for i in $(seq 0 $((n - 1))); do
            c="${configs[$(( (i + r - 1) % n ))]}"
            name="${c%%|*}"; args="${c#*|}"
            start=$(date +%s)
            code=$(run "${name}_r$r" $args)
            echo "$(date +%T) workers=$w round=$r $name exit=$code $(( $(date +%s) - start ))s" | tee -a "$OUT/progress.txt"
        done
    done
done
echo DONE >> "$OUT/progress.txt"
