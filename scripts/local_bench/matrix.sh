#!/usr/bin/env bash
# Runs every version of every operation in isolated JVMs, ROUNDS times, rotating the order each round
# so that no version always runs first (cold disk cache) or last.
# Each operation runs after M1 v3 (its input); M1 runs alone. ALL_v0 and ALL_best run the whole pipeline.
# usage: scripts/local_bench/matrix.sh            (ROUNDS=3, every config)
#        ROUNDS=1 ONLY='^M3_' scripts/local_bench/matrix.sh
# Then: scripts/local_bench/analyze.sh
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && (pwd -W 2>/dev/null || pwd))"
OUT="${BENCH_OUT:-$ROOT/build/local-bench}"
ROUNDS=${ROUNDS:-3}
ONLY=${ONLY:-.}
mkdir -p "$OUT"

# Spark reads every file of the data directory, so the Parquet files get a directory of their own (hard links, no copy).
if [ -z "$BENCH_DATA" ] && [ ! -d "$ROOT/data/bench" ]; then
    mkdir -p "$ROOT/data/bench"
    for f in "$ROOT"/data/yellow_tripdata_*.parquet; do ln "$f" "$ROOT/data/bench/" 2>/dev/null || cp "$f" "$ROOT/data/bench/"; done
fi
(cd "$ROOT" && ./gradlew -q localBenchClasspath) || exit 1

{
    echo "date: $(date -Iseconds)"
    echo "commit: $(git -C "$ROOT" rev-parse --short HEAD) $(git -C "$ROOT" status --porcelain | grep -q . && echo '(dirty)')"
    echo "os: $(uname -srm)"
    echo "cpus: $(nproc 2>/dev/null)"
    echo "data: ${BENCH_DATA:-$ROOT/data/bench} ($(ls "${BENCH_DATA:-$ROOT/data/bench}" | wc -l) files)"
    echo "heap: ${BENCH_HEAP:-4g}"
    "$(cat "$ROOT/build/local-bench/java.txt")" -version 2>&1
} > "$OUT/env.txt"

configs=()
for v in v0 v1 v2 v3; do configs+=("M1_$v|--ops M1 --m1 $v"); done
for v in v0 v1 v2 v3 v4 v5 v6 v7; do configs+=("M2_$v|--ops M1,M2 --m1 v3 --m2 $v"); done
for v in v0 v1 v2 v3; do configs+=("M3_$v|--ops M1,M3 --m1 v3 --m3 $v"); done
for v in v0 v1 v2 v3 v4 v5 v6 v7; do configs+=("O1_$v|--ops M1,O1 --m1 v3 --o1 $v"); done
configs+=("O4_v0|--ops M1,O4 --m1 v3 --o4 v0")
configs+=("ALL_v0|--ops M1,M2,M3,O1,O4 --m1 v0 --m2 v0 --m3 v0 --o1 v0 --o4 v0")
configs+=("ALL_best|--ops M1,M2,M3,O1,O4 --m1 v3 --m2 v7 --m3 v3 --o1 v7 --o4 v0")
n=${#configs[@]}

: > "$OUT/progress.txt"
for r in $(seq 1 "$ROUNDS"); do
    for i in $(seq 0 $((n - 1))); do
        c="${configs[$(( (i + (r - 1) * 7) % n ))]}"
        name="${c%%|*}"; args="${c#*|}"
        echo "$name" | grep -Eq "$ONLY" || continue
        start=$(date +%s)
        code=$(bash "$HERE/run1.sh" "${name}_r$r" $args)
        echo "$(date +%T) round=$r $name exit=$code $(( $(date +%s) - start ))s" | tee -a "$OUT/progress.txt"
    done
done
echo DONE >> "$OUT/progress.txt"
