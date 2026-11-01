package compar.ops.o1

import compar.ops.Analysis
import compar.ops.AnalysisInput
import compar.ops.common.pickupZones
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions as sf

/**
 * O1 v1 — Column pruning and single project projection.
 *
 * Bottleneck (v0):
 * - Multiple chained withColumn calls create consecutive Project nodes in Catalyst's logical plan.
 * - Missing explicit projection causes all 18+ input columns from Parquet (VendorID, tip_amount, etc.)
 *   to be read and kept in memory across subsequent operators.
 *
 * Change:
 * - Introduces selectTripMetrics(): uses a single explicit select() with column pruning to read
 *   only the 5 strictly required columns and compute derived metrics in a single pass.
 * - Reuses baseline window flagging, join, aggregation, and writes output to Parquet.
 */
class O1V1 : Analysis {

    override fun run(input: AnalysisInput): Dataset<Row> {
        println("\n--- [$tag] Fare Anomaly Detection ---")

        // 1. Column pruning: single project node selecting only required columns
        val dfWithMetrics = selectTripMetrics(input.trips)

        // 2 & 3. Compute bucket statistics (μ, σ) via Window and flag anomalous trips (> μ + 3σ)
        val dfFlagged = flagAnomaliesWithWindow(dfWithMetrics)

        // 4. Join with the zone lookup table (standard join without broadcast, preserving v0 behavior)
        val dfWithBorough = dfFlagged.join(pickupZones(input.zones), COL_PU_LOCATION_ID)

        // 5. Compare weekday vs. weekend anomaly rates by payment_type and pickup_borough
        val anomalyRatesDf = aggregateAnomalyRates(dfWithBorough)

        writeAnomalyRates(anomalyRatesDf, input.outputDir)

        return anomalyRatesDf
    }
}

/**
 * Projects only the 5 required trip columns and computes derived temporal and rate metrics
 * in a single Catalyst Project node (Column Pruning introduced in v1).
 */
internal fun selectTripMetrics(trips: Dataset<Row>): Dataset<Row> = trips.select(
    sf.col(COL_PU_LOCATION_ID),
    sf.col(COL_PAYMENT_TYPE),
    sf.hour(sf.col(COL_PICKUP_DATETIME)).alias(COL_HOUR_OF_DAY),
    sf.dayofweek(sf.col(COL_PICKUP_DATETIME)).alias(COL_DAY_OF_WEEK),
    dayTypeExpr(sf.dayofweek(sf.col(COL_PICKUP_DATETIME))).alias(COL_DAY_TYPE),
    farePerKm().alias(COL_FARE_PER_KM)
)
