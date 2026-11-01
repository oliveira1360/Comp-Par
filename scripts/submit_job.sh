#!/usr/bin/env bash
# Builds the Kotlin project (with Gradle) and submits the JAR to the Spark Master inside Docker
# Usage: ./scripts/submit_job.sh [--help] [--skip-build] [pipeline arguments...]

set -e

# Stops Git Bash (MSYS) from converting container paths (/opt, /apps, /data) to Windows paths
export MSYS_NO_PATHCONV=1

cd "$(dirname "$0")/.." || exit 1

show_help() {
    cat << 'EOF'
================================================================================
  NYC Yellow Taxi Analytics - Spark Job Submission (scripts/submit_job.sh)
================================================================================

Usage:
  ./scripts/submit_job.sh [script options] [pipeline arguments...]

Script Options:
  -h, --help, help        Show this detailed help message and exit.
  --skip-build, --no-build Do not compile the JAR with Gradle before submitting
                          (useful for faster iteration if the JAR is already built).

Kotlin Pipeline Arguments (MainKt):
  --ops <M1,M2,...>       Comma-separated list of operations to execute.
                          Available operations: M1, M2, M3, O1, O4
                          Default: M1,M2,M3,O1,O4
                          Example M1 only:       --ops M1
                          Example M1 and O1:     --ops M1,O1
                          Example M1, M2, M3:    --ops M1,M2,M3

  --m1 <version>          M1 implementation version (v0..v3, default: v3).
  --m2 <version>          M2 implementation version (v0..v7, default: v7).
  --m3 <version>          M3 implementation version (v0..v1, default: v0).
  --o1 <version>          O1 implementation version (v0..v1, default: v0).
  --o4 <version>          O4 implementation version (v0..v1, default: v0).

  --sample <fraction>     Data sampling fraction (between 0.0 and 1.0).
                          Default: 1.0 (processes complete dataset)
                          Example 0.1% (~3k):    --sample 0.001
                          Example 1%:            --sample 0.01
                          Example 10%:           --sample 0.1

  --profile               Enable SparkProfiler (measures stage time, shuffle, and memory).
  --data <path>           Path to input Parquet file(s) in Docker.
                          Default: /data/yellow_tripdata_2015-01.parquet
                          Supports glob patterns, e.g.: "/data/yellow_tripdata_2015-*.parquet"

  --lookup <path>         Path to taxi_zone_lookup.csv in Docker.
                          Default: /data/taxi_zone_lookup.csv

  --output <path>         Output directory for Parquet results in Docker.
                          Default: /output

  --export-metrics <path> JSON file path to export profiler metrics report.
                          Default: /output/profiling_report.json

  --master <url>          Spark Master URL (default: spark://spark-master:7077).

Examples:
  1. Quick test of M1 with 0.1% of data:
     ./scripts/submit_job.sh --sample 0.001 --ops M1

  2. Test M1 and O1 with profiler enabled and 1% of data:
     ./scripts/submit_job.sh --sample 0.01 --profile --ops M1,O1

  3. Run the 3 mandatory operations (M1, M2, M3) on month 01:
     ./scripts/submit_job.sh --ops M1,M2,M3

  4. Run with profiling on all files of 2015:
     ./scripts/submit_job.sh --data "/data/yellow_tripdata_2015-*.parquet" --profile

  5. Submit without rebuilding the JAR:
     ./scripts/submit_job.sh --skip-build --sample 0.01 --ops M1

Local Execution (Host without Docker):
  Run directly on host via Gradle:
     ./gradlew run --args="--master local[*] --sample 0.01 --profile --ops M1"
================================================================================
EOF
}

SKIP_BUILD=false
PASSED_ARGS=()

for arg in "$@"; do
    case "$arg" in
        -h|--help|help)
            show_help
            exit 0
            ;;
        --skip-build|--no-build)
            SKIP_BUILD=true
            ;;
        *)
            PASSED_ARGS+=("$arg")
            ;;
    esac
done

# Ensures JDK 17 is used if the system JDK is Java 25+
if [ -d "$HOME/.jdks/temurin-17" ] && [ -z "$JAVA_HOME" ]; then
    export JAVA_HOME="$HOME/.jdks/temurin-17"
    export PATH="$JAVA_HOME/bin:$PATH"
fi

JAR_PATH="build/libs/nyc-taxi-analytics.jar"

if [ "$SKIP_BUILD" = true ]; then
    if [ ! -f "$JAR_PATH" ]; then
        echo "[Warning] '$JAR_PATH' not found. Compiling with Gradle..."
        SKIP_BUILD=false
    else
        echo "[Info] Skipping Gradle build (--skip-build active)..."
    fi
fi

if [ "$SKIP_BUILD" = false ]; then
    if [ -x "./gradlew" ]; then
        GRADLE_OUTPUT=$(./gradlew jar --quiet 2>&1) || {
            echo "[Error] Gradle compilation failed:"
            echo "$GRADLE_OUTPUT"
            exit 1
        }
    else
        GRADLE_OUTPUT=$(./gradlew.bat jar --quiet 2>&1) || {
            echo "[Error] Gradle compilation failed:"
            echo "$GRADLE_OUTPUT"
            exit 1
        }
    fi
fi

if ! docker ps --format '{{.Names}}' | grep -qx spark-master; then
    echo "================================================================================"
    echo "Error: Container 'spark-master' is not running!"
    echo "Start the Spark cluster before submitting a job:"
    echo "  ./scripts/start_cluster.sh"
    echo "  (or: docker compose -f docker/docker-compose.yml up -d)"
    echo "================================================================================"
    exit 1
fi

TTY_FLAG="-i"
if [ -t 0 ]; then
    TTY_FLAG="-it"
fi

DEFAULT_ARGS=()
HAS_DATA=false
HAS_LOOKUP=false
HAS_OUTPUT=false

for arg in "${PASSED_ARGS[@]}"; do
    if [[ "$arg" == "--data" ]]; then HAS_DATA=true; fi
    if [[ "$arg" == "--lookup" ]]; then HAS_LOOKUP=true; fi
    if [[ "$arg" == "--output" ]]; then HAS_OUTPUT=true; fi
done

if [ "$HAS_DATA" = false ]; then
    DEFAULT_ARGS+=(--data "/data/yellow_tripdata_2015-01.parquet")
fi

if [ "$HAS_LOOKUP" = false ]; then
    DEFAULT_ARGS+=(--lookup "/data/taxi_zone_lookup.csv")
fi

if [ "$HAS_OUTPUT" = false ]; then
    DEFAULT_ARGS+=(--output "/output")
fi

echo "Submitting Spark job (Kotlin) to spark-master..."

docker exec $TTY_FLAG spark-master /opt/spark/bin/spark-submit \
    --class compar.MainKt \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /apps/nyc-taxi-analytics.jar \
    "${DEFAULT_ARGS[@]}" \
    "${PASSED_ARGS[@]}"
