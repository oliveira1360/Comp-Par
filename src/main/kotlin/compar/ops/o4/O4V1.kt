package compar.ops.o4

import compar.ops.Analysis
import compar.ops.AnalysisInput
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions as sf

/**
 * O4 v1 — Column pruning and single project projection.
 *
 * Bottleneck (v0):
 * - Chained withColumn() calls on input.trips create multiple consecutive Project nodes in
 *   Catalyst's logical plan on top of all ~20 input columns from M1 (VendorID, store_and_fwd_flag,
 *   extra, mta_tax, tip_amount, tolls_amount, etc.).
 * - Sharing a single unpruned dfPrepared across both branches forces the bucket classification
 *   branch to carry fare/distance/year_month columns and the monthly stats branch to carry
 *   RatecodeID and passenger_count columns.
 *
 * Change:
 * - Introduces selectO4TripColumns(): uses a single explicit select() with column pruning to
 *   project only the 7 required columns and compute hour_of_day and year_month in a single pass.
 * - Reuses classifyBucketsBaseline(), standard join, aggregateMonthlyZoneTypeStats(), and
 *   writeTripTypeStats() from v0.
 */
class O4V1(
    private val airportThreshold: Double = DEFAULT_AIRPORT_THRESHOLD,
    private val negotiatedThreshold: Double = DEFAULT_NEGOTIATED_THRESHOLD,
    private val groupThreshold: Double = DEFAULT_GROUP_THRESHOLD,
    private val commercialMaxGroup: Double = DEFAULT_COMMERCIAL_MAX_GROUP
) : Analysis {

    override fun run(input: AnalysisInput): Dataset<Row> {
        println("\n--- [$tag] Trip-Type Classification ---")

        // 1. Column pruning: single project node selecting only the 7 required columns + derived time columns
        val dfPrepared = selectO4TripColumns(input.trips)

        // 2. Compute classified (PULocationID, hour_of_day) buckets (reused from v0)
        val classifiedBuckets = classifyBucketsBaseline(
            dfPrepared = dfPrepared,
            airportThreshold = airportThreshold,
            negotiatedThreshold = negotiatedThreshold,
            groupThreshold = groupThreshold,
            commercialMaxGroup = commercialMaxGroup
        )

        // 3. Join the classification back to the trip table (preserving v0 join strategy)
        val tripsWithZoneType = dfPrepared
            .join(classifiedBuckets, arrayOf(COL_PU_LOCATION_ID, COL_HOUR_OF_DAY))

        // 4. Compute fare and distance statistics per zone type and month (reused from v0)
        val o4Stats = aggregateMonthlyZoneTypeStats(tripsWithZoneType)

        writeTripTypeStats(o4Stats, input.outputDir)

        return o4Stats
    }
}

/**
 * Projects only therequired trip columns for O4 and derives hour_of_day and year_month
 * in a single Catalyst Project node (Column Pruning introduced in v1).
 */
internal fun selectO4TripColumns(trips: Dataset<Row>): Dataset<Row> = trips.select(
    sf.col(COL_PU_LOCATION_ID),
    sf.col(COL_RATECODE_ID),
    sf.col(COL_PASSENGER_COUNT),
    sf.col(COL_FARE_AMOUNT),
    sf.col(COL_TRIP_DISTANCE),
    sf.col(COL_TOTAL_AMOUNT),
    sf.hour(sf.col(COL_PICKUP_DATETIME)).alias(COL_HOUR_OF_DAY),
    sf.date_format(sf.col(COL_PICKUP_DATETIME), "yyyy-MM").alias(COL_YEAR_MONTH)
)
