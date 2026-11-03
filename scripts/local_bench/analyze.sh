#!/usr/bin/env bash
# Aggregates the matrix results (compar.bench.AnalyzeKt): median metrics per version and result checks against v0.
# usage: scripts/local_bench/analyze.sh [bench_dir]      (default: build/local-bench)
#   -> prints the table and writes summary.csv and summary.md into bench_dir
ROOT="$(cd "$(dirname "$0")/../.." && (pwd -W 2>/dev/null || pwd))"
OUT="${1:-${BENCH_OUT:-$ROOT/build/local-bench}}"
(cd "$ROOT" && ./gradlew -q localBenchClasspath) || exit 1
O="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
"$(cat "$ROOT/build/local-bench/java.txt")" $O -Dsun.stdout.encoding=UTF-8 -cp "$(cat "$ROOT/build/local-bench/classpath.txt")" compar.bench.AnalyzeKt "$OUT" 2> "$OUT/analyze.err" || { tail -20 "$OUT/analyze.err"; exit 1; }
