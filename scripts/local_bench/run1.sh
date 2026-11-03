#!/usr/bin/env bash
# One isolated run of the real pipeline (compar.MainKt) in a fresh JVM, local[*].
# usage: run1.sh <run_id> <main args...>
#   -> $BENCH_OUT/logs/<run_id>.log, $BENCH_OUT/metrics/<run_id>.json, $BENCH_OUT/out/<run_id>/
# Prints the exit code of the JVM.
ROOT="$(cd "$(dirname "$0")/../.." && (pwd -W 2>/dev/null || pwd))"
OUT="${BENCH_OUT:-$ROOT/build/local-bench}"
DATA="${BENCH_DATA:-$ROOT/data/bench}"
HEAP="${BENCH_HEAP:-4g}"
ID=$1; shift

JAVA="$(cat "$ROOT/build/local-bench/java.txt")"
O="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
# Windows without winutils.exe: directory listing and Parquet writes need the native library, so use the java.io file system.
case "$(uname -s)" in
    MINGW*|MSYS*|CYGWIN*) FS="-Dspark.hadoop.fs.file.impl=compar.bench.NoNativeLocalFs -Dspark.hadoop.fs.file.impl.disable.cache=true" ;;
    *) FS="" ;;
esac

mkdir -p "$OUT/logs" "$OUT/metrics" "$OUT/out"
rm -rf "$OUT/out/$ID"
"$JAVA" "-Xmx$HEAP" $FS $O -cp "$(cat "$ROOT/build/local-bench/classpath.txt")" compar.MainKt \
    --master "local[*]" --data "$DATA" --lookup "$ROOT/data/taxi_zone_lookup.csv" \
    --output "$OUT/out/$ID" --export-metrics "$OUT/metrics/$ID.json" --profile "$@" > "$OUT/logs/$ID.log" 2>&1
echo $?
