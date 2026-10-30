#!/usr/bin/env bash
# Runs the pipeline on the Docker cluster several times (with --profile) and saves the times and result of each run.
#
# Usage:
#   ./scripts/benchmark.sh <LABEL> [N_RUNS] [DATA] [-- extra job arguments]
# Examples:
#   ./scripts/benchmark.sh baseline                                   # 3 runs, full year 2024
#   ./scripts/benchmark.sh m1-cache 5 "/data/yellow_tripdata_2024-0[1-3].parquet"
#   ./scripts/benchmark.sh amostra 3 "/data/yellow_tripdata_2024-*.parquet" -- --sample 0.1
#
# Results:
#   benchmarks/results.csv                         one line per run (accumulates across benchmarks)
#   benchmarks/logs/<date>_<label>_run<N>.log      full output of each run

set -e

# Stops Git Bash (MSYS) from converting container paths (/opt, /apps, /data) to Windows paths
export MSYS_NO_PATHCONV=1

LABEL=${1:?"Indica uma etiqueta para esta corrida (ex: baseline)"}
RUNS=${2:-3}
DATA=${3:-"/data/yellow_tripdata_2024-*.parquet"}
shift $(( $# < 3 ? $# : 3 ))
[ "$1" == "--" ] && shift
EXTRA_ARGS=("$@")

cd "$(dirname "$0")/.." || exit 1

# Ensures JDK 17 is used if the system JDK is Java 25+
if [ -d "$HOME/.jdks/temurin-17" ] && [ -z "$JAVA_HOME" ]; then
    export JAVA_HOME="$HOME/.jdks/temurin-17"
    export PATH="$JAVA_HOME/bin:$PATH"
fi

if ! docker ps --format '{{.Names}}' | grep -qx spark-master; then
    echo "O contentor spark-master não está a correr. Inicia o cluster com ./scripts/start_cluster.sh"
    exit 1
fi
WORKERS=$(docker ps --format '{{.Names}}' | grep -c spark-worker || true)

echo "A compilar o projeto Kotlin com Gradle..."
if [ -x "./gradlew" ]; then
    ./gradlew jar --quiet
else
    ./gradlew.bat jar --quiet
fi

mkdir -p benchmarks/logs
CSV=benchmarks/results.csv
if [ ! -f "$CSV" ]; then
    echo "timestamp,label,run,data,extra_args,workers,status,total_s,m1_s,m2_s,m3_s,m1_input,m2_input,m3_input,m1_disk_spill,m2_disk_spill,m3_disk_spill,total_records,valid_records,m3_flagged" > "$CSV"
fi

# Extracts a column from an operation's row in the SparkProfiler table (columns separated by "|")
profiler_col() {
    local log=$1 op=$2 col=$3
    grep -E "^${op} +\|" "$log" | tail -1 | awk -F'|' -v c="$col" '{gsub(/ /, "", $c); print $c}'
}

# Extracts the number from a line such as "[M1] Registos válidos: 8,400,459 (87.92%)"
count_after() {
    local log=$1 prefix=$2
    grep -F "$prefix" "$log" | tail -1 | sed -E 's/^[^:]*: *([0-9][0-9,]*).*/\1/' | tr -d ','
}

echo "=========================================================="
echo " Benchmark '$LABEL': $RUNS execução(ões), $WORKERS worker(s)"
echo " Dados: $DATA ${EXTRA_ARGS[*]}"
echo "=========================================================="

for run in $(seq 1 "$RUNS"); do
    TS=$(date +%Y%m%d-%H%M%S)
    LOG="benchmarks/logs/${TS}_${LABEL}_run${run}.log"

    echo " [>] Execução $run/$RUNS (log: $LOG)"
    set +e
    docker exec spark-master /opt/spark/bin/spark-submit \
        --class compar.MainKt \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        /apps/nyc-taxi-analytics.jar \
        --data "$DATA" \
        --lookup /data/taxi_zone_lookup.csv \
        --output /data/output \
        --profile \
        "${EXTRA_ARGS[@]}" > "$LOG" 2>&1
    EXIT_CODE=$?
    set -e

    if [ "$EXIT_CODE" -eq 0 ]; then STATUS=ok; else STATUS="erro($EXIT_CODE)"; fi

    TOTAL_S=$(grep -E "Tempo total decorrido" "$LOG" | tail -1 | sed -E 's/.*: ([0-9.,]+)s.*/\1/' | tr ',' '.')
    ROW="$(date -Iseconds),$LABEL,$run,\"$DATA\",\"${EXTRA_ARGS[*]}\",$WORKERS,$STATUS,$TOTAL_S"
    for col in 2 8 13; do
        for op in M1_ingestion_and_cleaning M2_temporal_demand M3_zone_statistics; do
            ROW="$ROW,$(profiler_col "$LOG" "$op" "$col")"
        done
    done
    ROW="$ROW,$(count_after "$LOG" "[M1] Total de registos lidos:")"
    ROW="$ROW,$(count_after "$LOG" "[M1] Registos válidos:")"
    ROW="$ROW,$(count_after "$LOG" "[M3] Total de zonas-mês com desvio > 1.5 std:")"
    echo "$ROW" >> "$CSV"

    echo "     estado=$STATUS total=${TOTAL_S:-?}s"
    if [ "$EXIT_CODE" -ne 0 ]; then
        echo "     Últimas linhas de erro:"
        grep -E "Exception|Error" "$LOG" | head -5 | sed 's/^/       /'
    fi
done

echo "=========================================================="
echo " Resultados de '$LABEL' (benchmarks/results.csv):"
grep -E "^[^,]+,${LABEL}," "$CSV" | cut -d, -f3,7-11 | sed 's/^/   run,status,total_s,m1_s,m2_s,m3_s = /'
echo "=========================================================="
