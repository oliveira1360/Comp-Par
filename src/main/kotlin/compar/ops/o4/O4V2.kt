package compar.ops.o4

import compar.ops.Analysis
import compar.ops.AnalysisInput
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions as sf

/**
 * O4 v2 — Broadcast join of the classified (PULocationID, hour_of_day) buckets.
 *
 * Bottleneck (v1):
 * - In v1, classifiedBuckets is the output of a groupBy(PULocationID, hour_of_day) aggregation.
 *   Because Spark does not know statically that PULocationID in [1, 263] and hour_of_day in [0, 23]
 *   yield at most 263 * 24 = 6,312 rows, the physical planner schedules a SortMergeJoin.
 * - As a result, Stage 7 hashes and shuffles 100% of the individual trip records (28.5M rows,
 *   344.0 MB of shuffle write/read on the 20% 2015 dataset) across the network so that each trip
 *   moves to the worker holding its (PULocationID, hour_of_day) classification.
 *
 * Change:
 * - Wraps classifiedBuckets in sf.broadcast(), forcing a BroadcastHashJoin.
 *   The tiny classifiedBuckets table (<= 6,312 rows, ~100 KB) is collected to the driver and
 *   broadcast to every worker's RAM, completely eliminating the 28.5M-row trip shuffle.
 * - Separates the column pruning projections for the two branches:
 *   1. selectBucketInputColumns() projects only the 4 columns needed to classify buckets.
 *   2. selectStatsInputColumns() projects only the 6 columns needed for the monthly statistics.
 */
class O4V2(
    private val airportThreshold: Double = DEFAULT_AIRPORT_THRESHOLD,
    private val negotiatedThreshold: Double = DEFAULT_NEGOTIATED_THRESHOLD,
    private val groupThreshold: Double = DEFAULT_GROUP_THRESHOLD,
    private val commercialMaxGroup: Double = DEFAULT_COMMERCIAL_MAX_GROUP
) : Analysis {

    override fun run(input: AnalysisInput): Dataset<Row> {
        println("\n--- [$tag] Trip-Type Classification ---")

        // 1. Branch-specific column pruning for bucket classification (only 4 columns, no year_month or fares)
        val dfBucketInput = selectBucketInputColumns(input.trips)

        // 2. Compute classified (PULocationID, hour_of_day) buckets (<= 6,312 rows)
        val classifiedBuckets = classifyBucketsBaseline(
            dfPrepared = dfBucketInput,
            airportThreshold = airportThreshold,
            negotiatedThreshold = negotiatedThreshold,
            groupThreshold = groupThreshold,
            commercialMaxGroup = commercialMaxGroup
        )

        // 3. Branch-specific column pruning for trips + Broadcast Join of the <= 6,312 classified buckets
        val dfStatsInput = selectStatsInputColumns(input.trips)
        val tripsWithZoneType = dfStatsInput
            .join(sf.broadcast(classifiedBuckets), arrayOf(COL_PU_LOCATION_ID, COL_HOUR_OF_DAY))

        // 4. Compute fare and distance statistics per zone type and month (reused from v0)
        val o4Stats = aggregateMonthlyZoneTypeStats(tripsWithZoneType)

        writeTripTypeStats(o4Stats, input.outputDir)

        return o4Stats
    }
}

/**
 * Projects only the 4 columns required by the bucket classification branch
 * (avoids reading fare_amount, trip_distance, total_amount or formatting year_month).
 */
internal fun selectBucketInputColumns(trips: Dataset<Row>): Dataset<Row> = trips.select(
    sf.col(COL_PU_LOCATION_ID),
    sf.hour(sf.col(COL_PICKUP_DATETIME)).alias(COL_HOUR_OF_DAY),
    sf.col(COL_RATECODE_ID),
    sf.col(COL_PASSENGER_COUNT)
)

/**
 * Projects only the 6 columns required by the monthly zone-type statistics branch
 * (avoids reading RatecodeID and passenger_count).
 */
internal fun selectStatsInputColumns(trips: Dataset<Row>): Dataset<Row> = trips.select(
    sf.col(COL_PU_LOCATION_ID),
    sf.hour(sf.col(COL_PICKUP_DATETIME)).alias(COL_HOUR_OF_DAY),
    sf.date_format(sf.col(COL_PICKUP_DATETIME), "yyyy-MM").alias(COL_YEAR_MONTH),
    sf.col(COL_FARE_AMOUNT),
    sf.col(COL_TRIP_DISTANCE),
    sf.col(COL_TOTAL_AMOUNT)
)
