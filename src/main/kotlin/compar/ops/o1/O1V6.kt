package compar.ops.o1

import compar.ops.Analysis
import compar.ops.AnalysisInput
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions as sf

/**
 * O1 v6 — Fused single-pass projection and integer-only aggregation accumulators with deferred
 * percentage calculation.
 *
 * Bottleneck (v5):
 * 1. Intermediate Projections: v5 materializes intermediate DataFrame columns (bucket_key, fare_per_km,
 *    is_anomaly) across consecutive Catalyst Project nodes before reaching HashAggregate.
 * 2. Redundant Floating-Point Accumulator in HashAggregate: aggregateByRateKey() maintains a 3rd
 *    accumulator — avg(when(is_anomaly, 1.0).otherwise(0.0)) — across millions of input trips.
 *    Internally, Spark's Average expression updates an extra Double sum and Long count on every trip,
 *    even though anomaly_rate_pct is algebraically determined by total_trips and anomaly_trips.
 *
 * Change:
 * 1. Introduces aggregateFusedWithDeferredRate(): fuses key packing, O(1) borough lookup, and O(1)
 *    threshold evaluation directly into groupBy(rate_key).agg(...) without materializing intermediate columns.
 * 2. Maintains only 2 integer accumulators (count and sum) inside HashAggregate, deferring the floating-point
 *    percentage division:
 *      anomaly_rate_pct = round((anomaly_trips * 100.0) / total_trips, 2)
 *    to run ONLY on the <= 84 aggregated output rows.
 * - Reuses buildDriverBoroughArray(), buildDriverThresholdArray(), isAnomalyFromArray(), and lookupBoroughId()
 *   from v5, and packBucketKey(), packRateKey(), and decodeRateKey() from v4.
 */
class O1V6 : Analysis {

    override fun run(input: AnalysisInput): Dataset<Row> {
        println("\n--- [$tag] Fare Anomaly Detection ---")

        // Step 1: Parse the 12 KB zone lookup CSV directly on the driver (reused from v5)
        val (boroughs, boroughLookupArray) = buildDriverBoroughArray(input.zones)

        // Step 2: Pre-aggregate bucket statistics and build the flat 67 KB threshold array (reused from v5)
        val thresholdLookupArray = buildDriverThresholdArray(input.trips)

        // Step 3: Fused aggregation with 2 integer accumulators and deferred rate division (introduced in v6)
        val aggregated = aggregateFusedWithDeferredRate(
            trips = input.trips,
            boroughLookupArray = boroughLookupArray,
            thresholdLookupArray = thresholdLookupArray
        )

        // Step 4: Decode rate_key on the <= 84 rows (reused from v4) and coalesce(1)
        val anomalyRatesDf = decodeRateKey(aggregated, boroughs)
            .orderBy(COL_PICKUP_BOROUGH, COL_PAYMENT_TYPE, COL_DAY_TYPE)
            .coalesce(1)

        writeAnomalyRates(anomalyRatesDf, input.outputDir)

        return anomalyRatesDf
    }
}

// ============================================================================
// Fused Aggregation Function (Introduced in v6)
// ============================================================================

/**
 * Fuses O(1) threshold lookup and O(1) borough key packing into a single groupBy().agg() pass.
 *
 * Inside HashAggregate, only two integer accumulators are maintained per group:
 *   - total_trips = Σ 1
 *   - anomaly_trips = Σ [is_anomaly == true]
 *
 * The floating-point division for anomaly_rate_pct is deferred until after aggregation,
 * executing at most 84 times instead of millions of times.
 */
internal fun aggregateFusedWithDeferredRate(
    trips: Dataset<Row>,
    boroughLookupArray: Column,
    thresholdLookupArray: Column,
    fareMetricExpr: Column = farePerKm()
): Dataset<Row> {
    // Inline 14-bit bucket_key and O(1) threshold lookup (reused from v4 & v5)
    val bucketKeyExpr = packBucketKey(sf.col(COL_PU_LOCATION_ID), sf.hour(sf.col(COL_PICKUP_DATETIME)))
    val isAnomalyExpr = isAnomalyFromArray(
        bucketKeyCol = bucketKeyExpr,
        fareMetricCol = fareMetricExpr,
        thresholdLookupArray = thresholdLookupArray
    )

    // Inline O(1) borough lookup and 8-bit rate_key packing (reused from v4 & v5)
    val boroughIdCol = lookupBoroughId(boroughLookupArray)
    val rateKeyExpr = packRateKey(boroughIdCol, sf.col(COL_PAYMENT_TYPE), sf.col(COL_PICKUP_DATETIME))

    return trips
        .select(*o1TripColumns)
        .groupBy(rateKeyExpr.alias(COL_RATE_KEY))
        .agg(
            sf.count("*").alias(COL_TOTAL_TRIPS),
            sf.sum(sf.`when`(isAnomalyExpr, 1).otherwise(0)).alias(COL_ANOMALY_TRIPS)
        )
        .withColumn(
            COL_ANOMALY_RATE_PCT,
            sf.round(
                sf.col(COL_ANOMALY_TRIPS).multiply(100.0).divide(sf.col(COL_TOTAL_TRIPS)),
                2
            )
        )
}
