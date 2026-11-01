package compar.ops.o1

import compar.ops.Analysis
import compar.ops.AnalysisInput
import compar.ops.common.pickupZones
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions as sf

/**
 * O1 — Fare Anomaly Detection:
 * 1. Computes the fare per km of each trip.
 * 2. Per (PULocationID, hour_of_day) bucket, computes the mean and standard deviation (σ).
 * 3. Flags trips whose fare per km exceeds mean + 3σ.
 * 4. Compares anomaly rates between weekdays and weekends,
 *    broken down by payment_type and pickup_borough.
 */
class O1V0Baseline : Analysis {

    private val kmPerMile = 1.60934

    override fun run(input: AnalysisInput): Dataset<Row> {
        println("\n--- [$tag] Fare Anomaly Detection ---")

        // 1. Derive the time columns and the fare per km
        val dfWithMetrics = input.trips
            .withColumn("hour_of_day", sf.hour(sf.col("tpep_pickup_datetime")))
            .withColumn("day_of_week", sf.dayofweek(sf.col("tpep_pickup_datetime")))
            .withColumn(
                "day_type",
                sf.`when`(sf.col("day_of_week").isin(1, 7), sf.lit("Weekend")).otherwise(sf.lit("Weekday"))
            )
            .withColumn(
                "fare_per_km",
                sf.col("fare_amount").divide(sf.col("trip_distance").multiply(kmPerMile))
            )

        // 2. Window partition per (PULocationID, hour_of_day) bucket to compute mean and sigma
        val wBucket = Window.partitionBy("PULocationID", "hour_of_day")

        val dfWithBucketStats = dfWithMetrics
            .withColumn("mean_fare_per_km", sf.avg("fare_per_km").over(wBucket))
            .withColumn("stddev_fare_per_km", sf.stddev("fare_per_km").over(wBucket))

        // 3. Flag anomalous trips: fare_per_km > mean + 3*stddev
        val thresholdExpr = sf.col("mean_fare_per_km").plus(sf.col("stddev_fare_per_km").multiply(3.0))
        val isAnomalyExpr = sf.coalesce(
            sf.col("stddev_fare_per_km").gt(0.0).and(sf.col("fare_per_km").gt(thresholdExpr)),
            sf.lit(false)
        )

        val dfFlagged = dfWithBucketStats.withColumn("is_anomaly", isAnomalyExpr)

        // 4. Join with the zone table to get the pickup borough
        val dfWithBorough = dfFlagged.join(pickupZones(input.zones), "PULocationID")

        // 5. Compare weekday and weekend anomaly rates, broken down by payment_type and pickup_borough
        val anomalyRatesDf = dfWithBorough
            .groupBy("pickup_borough", "payment_type", "day_type")
            .agg(
                sf.count("*").alias("total_trips"),
                sf.sum(sf.`when`(sf.col("is_anomaly"), 1).otherwise(0)).alias("anomaly_trips"),
                sf.round(
                    sf.avg(sf.`when`(sf.col("is_anomaly"), 1.0).otherwise(0.0)).multiply(100.0),
                    2
                ).alias("anomaly_rate_pct")
            )
            .orderBy("pickup_borough", "payment_type", "day_type")

        anomalyRatesDf.show(30, false)

        val totalAnomalies = dfFlagged.filter(sf.col("is_anomaly").equalTo(true)).count()
        println(String.format("[$tag] Total trips flagged as anomalies (> 3σ): %,d", totalAnomalies))

        return anomalyRatesDf
    }
}
