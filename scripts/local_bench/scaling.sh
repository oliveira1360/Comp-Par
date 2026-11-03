#!/usr/bin/env bash
# Local scalability runs of the optimised pipeline (ALL_best), ROUNDS times each, in fresh JVMs:
#   data size:  1, 3, 6 and 12 months of 2024 (ALL-m<n>), and --sample 0.1 / 0.2 on the 12 months (ALL-f<pct>)
#   cores:      local[1], local[2], local[4], local[8] on the 12 months (ALL-c<n>)
# usage: scripts/local_bench/scaling.sh      then: scripts/local_bench/analyze.sh build/local-bench-scaling
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && (pwd -W 2>/dev/null || pwd))"
export BENCH_OUT="${BENCH_OUT:-$ROOT/build/local-bench-scaling}"
ROUNDS=${ROUNDS:-3}
BEST="--ops M1,M2,M3,O1,O4 --m1 v3 --m2 v7 --m3 v3 --o1 v7 --o4 v0"
mkdir -p "$BENCH_OUT"

# One data directory per number of months (hard links, no copy).
for n in 1 3 6 12; do
    d="$ROOT/data/bench_m$n"
    [ -d "$d" ] && continue
    mkdir -p "$d"
    for m in $(seq 1 "$n"); do
        f="$ROOT/data/yellow_tripdata_2024-$(printf %02d "$m").parquet"
        ln "$f" "$d/" 2>/dev/null || cp "$f" "$d/"
    done
done
(cd "$ROOT" && ./gradlew -q localBenchClasspath) || exit 1

configs=()
for n in 1 3 6 12; do configs+=("ALL-m${n}_best|$ROOT/data/bench_m$n|$BEST"); done
for p in 10 20; do configs+=("ALL-f${p}_best|$ROOT/data/bench_m12|$BEST --sample 0.$p"); done
for c in 1 2 4 8; do configs+=("ALL-c${c}_best|$ROOT/data/bench_m12|$BEST --master local[$c]"); done
n=${#configs[@]}

: > "$BENCH_OUT/progress.txt"
for r in $(seq 1 "$ROUNDS"); do
    for i in $(seq 0 $((n - 1))); do
        c="${configs[$(( (i + (r - 1) * 3) % n ))]}"
        name="${c%%|*}"; rest="${c#*|}"; data="${rest%%|*}"; args="${rest#*|}"
        start=$(date +%s)
        code=$(BENCH_DATA="$data" bash "$HERE/run1.sh" "${name}_r$r" $args)
        echo "$(date +%T) round=$r $name exit=$code $(( $(date +%s) - start ))s" | tee -a "$BENCH_OUT/progress.txt"
    done
done
echo DONE >> "$BENCH_OUT/progress.txt"
