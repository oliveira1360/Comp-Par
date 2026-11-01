package compar.ops.o1

import compar.ops.Analysis
import compar.ops.AnalysisInput
import compar.ops.common.pickupZones
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions as sf

/**
 * O1 — Fare Anomaly Detection (Baseline):
 * 1. Derives temporal metrics and fare per km:
 *    fare_per_km = fare_amount / (trip_distance * 1.60934)
 * 2. Per (PULocationID, hour_of_day) bucket, computes the mean (μ) and standard deviation (σ)
 *    over an unpartitioned Spark Window:
 *    - Mean: μ = (1 / N) * Σ x_i
 *    - Sample Stddev: σ = sqrt((1 / (N - 1)) * Σ (x_i - μ)^2)
 * 3. Flags trips as anomalous if fare_per_km > μ + 3σ (where σ > 0).
 * 4. Joins with the zone lookup table to obtain pickup_borough.
 * 5. Compares weekday vs. weekend anomaly rates broken down by payment_type and pickup_borough:
 *    anomaly_rate_pct = (anomaly_trips / total_trips) * 100%
 * Writes the resulting rates in Parquet format to output/o1_fare_anomalies.
 */
class O1V0Baseline : Analysis {

    override fun run(input: AnalysisInput): Dataset<Row> {
        println("\n--- [$tag] Fare Anomaly Detection ---")

        // 1. Derive temporal columns and fare per km (baseline with chained withColumn calls)
        val dfWithMetrics = input.trips
            .withColumn(COL_HOUR_OF_DAY, sf.hour(sf.col(COL_PICKUP_DATETIME)))
            .withColumn(COL_DAY_OF_WEEK, sf.dayofweek(sf.col(COL_PICKUP_DATETIME)))
            .withColumn(COL_DAY_TYPE, dayTypeExpr())
            .withColumn(COL_FARE_PER_KM, farePerKm())

        // 2 & 3. Compute bucket statistics (μ, σ) via Window and flag anomalous trips (> μ + 3σ)
        val dfFlagged = flagAnomaliesWithWindow(dfWithMetrics)

        // 4. Join with the zone lookup table to obtain pickup_borough
        val dfWithBorough = dfFlagged.join(pickupZones(input.zones), COL_PU_LOCATION_ID)

        // 5. Compare weekday vs. weekend anomaly rates by payment_type and pickup_borough
        val anomalyRatesDf = aggregateAnomalyRates(dfWithBorough)

        writeAnomalyRates(anomalyRatesDf, input.outputDir)

        return anomalyRatesDf
    }
}

/**
 * Computes mean (μ) and sample standard deviation (σ) of fare_per_km per (PULocationID, hour_of_day) bucket
 * using Spark Window functions, and flags anomalous records exceeding μ + 3σ.
 *
 * Mathematical formulas:
 * - Metric: x_i = fare_amount / (trip_distance * 1.60934)
 * - Bucket Mean: μ = (1 / N) * Σ x_i
 * - Sample Standard Deviation: σ = sqrt((1 / (N - 1)) * Σ (x_i - μ)^2)
 * - Anomaly Condition: is_anomaly = (x_i > μ + 3 * σ) ∧ (σ > 0)
 */
internal fun flagAnomaliesWithWindow(df: Dataset<Row>): Dataset<Row> {
    val wBucket = Window.partitionBy(COL_PU_LOCATION_ID, COL_HOUR_OF_DAY)

    val withBucketStats = df
        .withColumn(COL_MEAN_FARE_PER_KM, sf.avg(COL_FARE_PER_KM).over(wBucket))
        .withColumn(COL_STDDEV_FARE_PER_KM, sf.stddev(COL_FARE_PER_KM).over(wBucket))

    val isAnomaly = anomalyCondition(
        fareCol = sf.col(COL_FARE_PER_KM),
        meanCol = sf.col(COL_MEAN_FARE_PER_KM),
        stddevCol = sf.col(COL_STDDEV_FARE_PER_KM)
    )

    return withBucketStats.withColumn(COL_IS_ANOMALY, isAnomaly)
}

/**
 * Aggregates trips by (pickup_borough, payment_type, day_type) to compute total volume,
 * anomalous trip volume, and the percentage anomaly rate.
 *
 * Mathematical formulas:
 * - Total Trips: N_group = Σ 1
 * - Anomalous Trips: N_anomaly = Σ [is_anomaly == true]
 * - Anomaly Rate (%): anomaly_rate_pct = round((N_anomaly / N_group) * 100, 2)
 *   Computed via Spark as round(avg(when(is_anomaly, 1.0).otherwise(0.0)) * 100.0, 2).
 */
internal fun aggregateAnomalyRates(dfWithBorough: Dataset<Row>): Dataset<Row> = dfWithBorough
    .groupBy(COL_PICKUP_BOROUGH, COL_PAYMENT_TYPE, COL_DAY_TYPE)
    .agg(
        sf.count("*").alias(COL_TOTAL_TRIPS),
        sf.sum(sf.`when`(sf.col(COL_IS_ANOMALY), 1).otherwise(0)).alias(COL_ANOMALY_TRIPS),
        sf.round(
            sf.avg(sf.`when`(sf.col(COL_IS_ANOMALY), 1.0).otherwise(0.0)).multiply(100.0),
            2
        ).alias(COL_ANOMALY_RATE_PCT)
    )
    .orderBy(COL_PICKUP_BOROUGH, COL_PAYMENT_TYPE, COL_DAY_TYPE)
