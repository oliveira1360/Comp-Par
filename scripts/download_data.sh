#!/usr/bin/env bash
# Downloads monthly NYC taxi Parquet files (TLC Yellow Taxi) to the data/ directory
#
# Usage:
#   ./scripts/download_data.sh [YEAR] [START_MONTH] [END_MONTH]
# Example:
#   ./scripts/download_data.sh 2024 2 3   # Downloads February and March 2024

set -e

YEAR=${1:-2024}
START_MONTH=${2:-1}
END_MONTH=${3:-3}

cd "$(dirname "$0")/.." || exit 1
mkdir -p data

BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"
LOOKUP_URL="https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

# Ensures the zone lookup table is present
if [ ! -f "data/taxi_zone_lookup.csv" ]; then
    echo "Downloading taxi_zone_lookup.csv..."
    curl -f -L -o "data/taxi_zone_lookup.csv" "$LOOKUP_URL"
    echo "taxi_zone_lookup.csv downloaded."
fi

echo "=========================================================="
echo " Downloading Yellow Taxi Parquet files for year $YEAR (Months $START_MONTH to $END_MONTH)"
echo " Destination: data/"
echo "=========================================================="

for m in $(seq "$START_MONTH" "$END_MONTH"); do
    MONTH_PAD=$(printf "%02d" "$m")
    FILENAME="yellow_tripdata_${YEAR}-${MONTH_PAD}.parquet"
    TARGET="data/${FILENAME}"
    URL="${BASE_URL}/${FILENAME}"

    if [ -f "$TARGET" ]; then
        echo " $FILENAME already exists in data/. Skipping..."
    else
        echo " Downloading $FILENAME from $URL..."
        if curl -f -L -o "$TARGET" "$URL"; then
            echo " Completed: $FILENAME ($(du -h "$TARGET" | cut -f1))"
        else
            echo " Error or file not available for ${YEAR}-${MONTH_PAD}"
            rm -f "$TARGET"
        fi
    fi
done

echo "=========================================================="
echo " Parquet files in data/:"
ls -lh data/*.parquet 2>/dev/null || echo "(No files found)"
echo "=========================================================="
