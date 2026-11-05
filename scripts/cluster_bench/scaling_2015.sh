#!/usr/bin/env bash
# Scalability runs for the report: the optimised pipeline (best version of each operation) on the 2015 data,
# through scripts/benchmark.sh, 3 runs per condition.
#   dataset scale: full data, 10 % and 20 % sample, with 2 workers
#   cluster size:  full data with 1, 2 and 3 workers (2 cores, 2 GB each)
# Metrics are moved from output/metrics/all/ to output/scaling/<condition>/ after each condition, so they do not mix
# with the per-version metrics that the notebook reads.
set -euo pipefail
cd "$(dirname "$0")/../.."

BEST=(--ops M1,M2,M3,O1,O4 --m1 v2 --m2 v4 --m3 v3 --o1 v6 --o4 v6 --runs 3)
mkdir -p output/scaling

run() {  # run <condition> [extra benchmark.sh args...]
    local cond="$1"; shift
    echo "=== $(date +%H:%M:%S) condition $cond ==="
    rm -rf output/metrics/all
    bash scripts/benchmark.sh "${BEST[@]}" "$@"
    rm -rf "output/scaling/$cond"
    mv output/metrics/all "output/scaling/$cond"
}

bash scripts/start_cluster.sh 2
run w2_full
run w2_s10 --sample 0.1
run w2_s20 --sample 0.2
bash scripts/start_cluster.sh 1
run w1_full
bash scripts/start_cluster.sh 3
run w3_full
bash scripts/start_cluster.sh 2
echo "=== $(date +%H:%M:%S) done ==="
