package compar.ops.o1

import compar.ops.Analysis
import compar.ops.AnalysisInput
import compar.ops.common.pickupZones
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions as sf

/**
 * O1 v3 — Window elimination via groupBy pre-aggregation and broadcast join.
 *
 * Bottleneck (v2):
 * - flagAnomaliesWithWindow uses Window.partitionBy(COL_PU_LOCATION_ID, COL_HOUR_OF_DAY).
 * - Window functions cannot perform map-side partial aggregation (map-side combine).
 *   As a result, 100% of individual trip records (millions of rows) are shuffled across the network.
 * - Busy zones (e.g. Manhattan at peak hours) suffer from severe data skew, concentrating hundreds
 *   of thousands of trips on a single worker and causing high task durations and memory pressure.
 *
 * Change:
 * - Replaces the Window function with flagAnomaliesWithGroupBy():
 *   1. Computes bucket statistics (μ, σ) using groupBy(COL_PU_LOCATION_ID, COL_HOUR_OF_DAY).agg().
 *      This leverages Spark's HashAggregate with local map-side combine, condensing millions of trips
 *      into at most 263 * 24 = 6,312 rows before shuffle (>98% shuffle reduction).
 *   2. Broadcasts the small bucket thresholds table (<= 6,312 rows, ~150 KB) back to the trips
 *      using sf.broadcast(). Trips remain in their local partitions with zero shuffle for flagging.
 * - Reuses selectTripMetrics() from v1, broadcast zone lookup from v2, aggregateAnomalyRates() from v0,
 *   and writes output to Parquet.
 */
class O1V3 : Analysis {

    override fun run(input: AnalysisInput): Dataset<Row> {
        println("\n--- [$tag] Fare Anomaly Detection ---")

        // 1. Column pruning (reused from v1)
        val dfWithMetrics = selectTripMetrics(input.trips)

        // 2 & 3. Window elimination: map-side pre-aggregation + broadcast join of bucket thresholds (introduced in v3)
        val dfFlagged = flagAnomaliesWithGroupBy(dfWithMetrics)

        // 4. Broadcast join on the 265-row zone table (reused from v2)
        val dfWithBorough = dfFlagged.join(sf.broadcast(pickupZones(input.zones)), COL_PU_LOCATION_ID)

        // 5. Compare weekday vs. weekend anomaly rates (reused from v0)
        val anomalyRatesDf = aggregateAnomalyRates(dfWithBorough)

        writeAnomalyRates(anomalyRatesDf, input.outputDir)

        return anomalyRatesDf
    }
}

/**
 * Computes bucket statistics (μ, σ) via groupBy with map-side combine and flags anomalies
 * by broadcasting the resulting small lookup table (<= 6,312 rows) back to the trips.
 *
 * Eliminates the expensive shuffle and data skew caused by Window.partitionBy.
 */
internal fun flagAnomaliesWithGroupBy(dfWithMetrics: Dataset<Row>): Dataset<Row> {
    // Step 2A: Pre-aggregate bucket statistics with map-side combine (<= 263 * 24 = 6,312 rows)
    val bucketStats = dfWithMetrics
        .groupBy(COL_PU_LOCATION_ID, COL_HOUR_OF_DAY)
        .agg(
            sf.avg(COL_FARE_PER_KM).alias(COL_MEAN_FARE_PER_KM),
            sf.stddev(COL_FARE_PER_KM).alias(COL_STDDEV_FARE_PER_KM)
        )

    // Step 2B: Broadcast join bucket thresholds back to trips without shuffling the trips
    val isAnomaly = anomalyCondition(
        fareCol = sf.col(COL_FARE_PER_KM),
        meanCol = sf.col(COL_MEAN_FARE_PER_KM),
        stddevCol = sf.col(COL_STDDEV_FARE_PER_KM)
    )

    return dfWithMetrics
        .join(sf.broadcast(bucketStats), arrayOf(COL_PU_LOCATION_ID, COL_HOUR_OF_DAY))
        .withColumn(COL_IS_ANOMALY, isAnomaly)
}
