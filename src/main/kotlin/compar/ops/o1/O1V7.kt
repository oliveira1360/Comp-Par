package compar.ops.o1

import compar.ops.Analysis
import compar.ops.AnalysisInput
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions as sf

/**
 * O1 v7 — Algebraic scale invariance of the (μ + 3σ) anomaly threshold.
 *
 * Bottleneck (v6):
 * - Both the bucket pre-aggregation pass and the fused trip aggregation pass evaluate
 *   farePerKm() = fare_amount / (trip_distance * 1.60934), performing an extra floating-point
 *   multiplication by 1.60934 on every single trip across both dataset scans (300M+ operations
 *   on the full dataset).
 * - However, the final output schema of O1 (pickup_borough, payment_type, day_type, total_trips,
 *   anomaly_trips, anomaly_rate_pct) does NOT expose the intermediate unit rate — it only evaluates
 *   the boolean outlier condition: y_i > μ_Y + 3 * σ_Y.
 *
 * Mathematical Proof (Scale Invariance of k-Sigma Outlier Detection):
 * - Let X = fare_amount / trip_distance (fare per mile) and Y = c * X, where c = 1 / 1.60934 > 0.
 * - By linearity of the mean and positive homogeneity of sample standard deviation for c > 0:
 *     μ_Y = (1 / N) * Σ (c * x_i) = c * μ_X
 *     σ_Y = sqrt((1 / (N - 1)) * Σ (c * x_i - c * μ_X)^2) = c * σ_X
 * - Substituting into the 3-sigma threshold inequality:
 *     y_i > μ_Y + 3 * σ_Y  <==>  c * x_i > c * (μ_X + 3 * σ_X)  <==>  x_i > μ_X + 3 * σ_X
 * - Because c > 0 cancels out identically on both sides, flagging anomalies directly on
 *   X = fare_amount / trip_distance is mathematically equivalent and eliminates the per-row
 *   unit conversion overhead across the entire pipeline.
 *
 * Change:
 * - Introduces farePerDistanceUnit() = fare_amount / trip_distance and buildScaleInvariantThresholdArray().
 * - Reuses buildDriverBoroughArray() and buildDriverThresholdArray() from v5,
 *   aggregateFusedWithDeferredRate() from v6, and decodeRateKey() from v4.
 */
class O1V7 : Analysis {

    override fun run(input: AnalysisInput): Dataset<Row> {
        println("\n--- [$tag] Fare Anomaly Detection ---")

        // Step 1: Parse the 12 KB zone lookup CSV directly on the driver (reused from v5)
        val (boroughs, boroughLookupArray) = buildDriverBoroughArray(input.zones)

        // Step 2: Build the O(1) threshold array using scale-invariant fare/distance (introduced in v7)
        val thresholdLookupArray = buildScaleInvariantThresholdArray(input.trips)

        // Step 3: Fused aggregation using scale-invariant farePerDistanceUnit() (reused from v6)
        val aggregated = aggregateFusedWithDeferredRate(
            trips = input.trips,
            boroughLookupArray = boroughLookupArray,
            thresholdLookupArray = thresholdLookupArray,
            fareMetricExpr = farePerDistanceUnit()
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
// Scale-Invariant Metric & Threshold Functions (Introduced in v7)
// ============================================================================

/**
 * Scale-invariant fare rate: x_i = fare_amount / trip_distance.
 * Avoids multiplying trip_distance by 1.60934 on every trip record.
 */
internal fun farePerDistanceUnit(): Column =
    sf.col(COL_FARE_AMOUNT).divide(sf.col(COL_TRIP_DISTANCE))

/**
 * Pre-aggregates bucket statistics (μ_X, σ_X) on the scale-invariant ratio (fare_amount / trip_distance)
 * and builds the flat 67 KB O(1) threshold array on the driver by reusing buildDriverThresholdArray() from v5.
 */
internal fun buildScaleInvariantThresholdArray(trips: Dataset<Row>): Column =
    buildDriverThresholdArray(trips, metricExpr = farePerDistanceUnit())
