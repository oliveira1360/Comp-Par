#!/usr/bin/env bash
# Compiles the Kotlin project once (with Gradle) and runs isolated benchmarks across one or more versions
# on the Docker Spark cluster via `./gradlew benchmark`.
# Usage: ./scripts/benchmark.sh [--help] [options...]

set -e

# Stops Git Bash (MSYS) from converting container paths (/opt, /apps, /data) to Windows paths
export MSYS_NO_PATHCONV=1

cd "$(dirname "$0")/.." || exit 1

show_help() {
    cat << 'EOF'
================================================================================
  NYC Yellow Taxi Analytics - Isolated Cluster Benchmark (scripts/benchmark.sh)
================================================================================

Usage:
  ./scripts/benchmark.sh [options...]

Options:
  -h, --help, help        Show this detailed help message and exit.

  --all                   Run all operations (M1, M2, M3, O1, O4) sequentially
                          with a cooldown between them (default: 20s).
                          Example: ./scripts/benchmark.sh --all --data "/data/yellow_tripdata_2015-01.parquet"

  --cooldown <sec>        Cooldown pause in seconds to let CPU rest (default: 20).
                          Can be set to 0 to disable: --cooldown 0

  --m1 <versions>         Comma-separated list of M1 versions to test (or "all").
                          Available: v0, v1, v2, v3 (default: v3)
                          Example: --m1 v1,v2,v3   or   --m1 all

  --m2 <versions>         Comma-separated list of M2 versions to test (or "all").
                          Available: v0, v1, v2, v3, v4, v5, v6, v7 (default: v7)
                          Example: --m2 v4,v5,v6,v7   or   --m2 all

  --m3 <versions>         Comma-separated list of M3 versions to test (or "all").
                          Available: v0, v1, v2, v3 (default: v3)
                          Example: --m3 v1,v2,v3   or   --m3 all

  --o1 <versions>         Comma-separated list of O1 versions to test (or "all").
                          Available: v0, v1, v2, v3, v4, v5, v6, v7 (default: v0)

  --o4 <versions>         Comma-separated list of O4 versions to test (or "all").
                          Available: v0, v1, v2, v3, v4, v5, v6 (default: v0)

  --ops <M1,M2,...>       Comma-separated list of operations to execute.
                          If omitted, inferred automatically from the version flags passed
                          (e.g. passing --m2 v5,v6,v7 automatically sets --ops M2).

  --sample <fraction>     Data sampling fraction (between 0.0 and 1.0).
                          Example 10%: --sample 0.1

  --data <path>           Path/glob to input Parquet file(s) inside Docker.
                          Default: /data/yellow_tripdata_2015-*.parquet
                          All years: --data "/data/yellow_tripdata_*.parquet"

  --runs <N>              Number of isolated repetitions per version (default: 1).

Examples:
  1. Compare M2 versions v4, v5, v6, v7 on 10% of all years:
     ./scripts/benchmark.sh --m2 v4,v5,v6,v7 --sample 0.1 --data "/data/yellow_tripdata_*.parquet"

  2. Compare all M2 versions (v0 through v7) on 10% sample:
     ./scripts/benchmark.sh --m2 all --sample 0.1 --data "/data/yellow_tripdata_*.parquet"

  3. Compare M1 versions (v1, v2, v3) on 2015 data:
     ./scripts/benchmark.sh --m1 v1,v2,v3

  4. Run 3 isolated repetitions of M2 v7:
     ./scripts/benchmark.sh --m2 v7 --runs 3 --sample 0.1 --data "/data/yellow_tripdata_*.parquet"

Direct Gradle Invocation:
  ./gradlew benchmark -Pm2=v4,v5,v6,v7 -Psample=0.1 -Pdata="/data/yellow_tripdata_*.parquet"
================================================================================
EOF
}

GRADLE_PROPS=()
FORWARD_ARGS=()
RUN_ALL_OPS=false
COOLDOWN=20

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help|help)
            show_help
            exit 0
            ;;
        --all|all)
            RUN_ALL_OPS=true
            shift
            ;;
        --cooldown)
            COOLDOWN="$2"
            GRADLE_PROPS+=("-Pcooldown=$2")
            FORWARD_ARGS+=("--cooldown" "$2")
            shift 2
            ;;
        --m1)
            GRADLE_PROPS+=("-Pm1=$2")
            shift 2
            ;;
        --m2)
            GRADLE_PROPS+=("-Pm2=$2")
            shift 2
            ;;
        --m3)
            GRADLE_PROPS+=("-Pm3=$2")
            shift 2
            ;;
        --o1)
            GRADLE_PROPS+=("-Po1=$2")
            shift 2
            ;;
        --o4)
            GRADLE_PROPS+=("-Po4=$2")
            shift 2
            ;;
        --ops|--operations)
            GRADLE_PROPS+=("-Pops=$2")
            shift 2
            ;;
        --sample)
            GRADLE_PROPS+=("-Psample=$2")
            FORWARD_ARGS+=("--sample" "$2")
            shift 2
            ;;
        --data)
            GRADLE_PROPS+=("-Pdata=$2")
            FORWARD_ARGS+=("--data" "$2")
            shift 2
            ;;
        --runs)
            GRADLE_PROPS+=("-Pruns=$2")
            FORWARD_ARGS+=("--runs" "$2")
            shift 2
            ;;
        -P*)
            GRADLE_PROPS+=("$1")
            shift
            ;;
        *)
            echo "Warning: unknown argument ignored: $1" >&2
            shift
            ;;
    esac
done

if [ "$RUN_ALL_OPS" = true ]; then
    echo "================================================================================"
    echo "  Running Complete Benchmark Suite (M1, M2, M3, O1, O4)"
    echo "  Cooldown between operations: ${COOLDOWN}s"
    echo "================================================================================"
    ALL_OPS=(m1 m2 m3 o1 o4)
    for idx in "${!ALL_OPS[@]}"; do
        op="${ALL_OPS[$idx]}"
        echo -e "\n================================================================================"
        echo "  [$(($idx + 1))/${#ALL_OPS[@]}] Benchmarking $op (all versions)"
        echo "================================================================================"
        bash "./scripts/benchmark.sh" "--$op" all "${FORWARD_ARGS[@]}" || exit 1
        if [ "$idx" -lt "$((${#ALL_OPS[@]} - 1))" ] && [ "$COOLDOWN" -gt 0 ]; then
            echo -e "\n>>> Cooling down: waiting ${COOLDOWN}s before next operation..."
            sleep "$COOLDOWN"
        fi
    done
    echo -e "\n================================================================================"
    echo "  All operations benchmarked successfully!"
    echo "================================================================================"
    exit 0
fi

# Ensures JDK 17 is used if the system JDK is Java 25+
if [ -d "$HOME/.jdks/temurin-17" ] && [ -z "$JAVA_HOME" ]; then
    export JAVA_HOME="$HOME/.jdks/temurin-17"
    export PATH="$JAVA_HOME/bin:$PATH"
fi

if ! docker ps --format '{{.Names}}' | grep -qx spark-master; then
    echo "================================================================================"
    echo "Error: Container 'spark-master' is not running!"
    echo "Start the Spark cluster before running benchmarks:"
    echo "  ./scripts/start_cluster.sh"
    echo "================================================================================"
    exit 1
fi

if [ -x "./gradlew" ]; then
    ./gradlew benchmark --console=plain "${GRADLE_PROPS[@]}"
else
    ./gradlew.bat benchmark --console=plain "${GRADLE_PROPS[@]}"
fi
