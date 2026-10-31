package compar.ops.m2

import compar.ops.Analysis
import compar.ops.AnalysisInput
import compar.ops.common.pickupZones
import compar.ops.common.tripDurationMin
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions as sf

/**
 * M2 — Temporal Demand Analysis:
 * For each (pickup_borough, day_of_week, hour_of_day) triple, computes:
 * - total trip count (total_trips)
 * - mean fare_amount (mean_fare_amount)
 * - mean trip duration in minutes (mean_trip_duration_min)
 * - mean distance (mean_trip_distance)
 * Writes the result partitioned by pickup_borough in Parquet format.
 */
class M2V0Baseline : Analysis {

    override fun run(input: AnalysisInput): Dataset<Row> {
        println("\n--- [M2] Temporal Demand Analysis ---")

        val m2Df = input.trips
            .join(pickupZones(input.zones), "PULocationID")
            .let(::aggregateTemporalDemand)
            .orderBy("pickup_borough", "day_of_week", "hour_of_day")

        writeTemporalDemand(m2Df, input.outputDir)

        return m2Df
    }
}

internal val m2TripColumns = arrayOf(
    sf.col("PULocationID"),
    sf.col("tpep_pickup_datetime"),
    sf.col("tpep_dropoff_datetime"),
    sf.col("fare_amount"),
    sf.col("trip_distance")
)

internal fun aggregateTemporalDemand(df: Dataset<Row>): Dataset<Row> = df
    .withColumn("day_of_week", sf.dayofweek(sf.col("tpep_pickup_datetime")))
    .withColumn("hour_of_day", sf.hour(sf.col("tpep_pickup_datetime")))
    .withColumn("trip_duration_min", tripDurationMin())
    .groupBy("pickup_borough", "day_of_week", "hour_of_day")
    .agg(
        sf.count("*").alias("total_trips"),
        sf.avg("fare_amount").alias("mean_fare_amount"),
        sf.avg("trip_duration_min").alias("mean_trip_duration_min"),
        sf.avg("trip_distance").alias("mean_trip_distance")
    )

internal fun writeTemporalDemand(df: Dataset<Row>, outputDir: String) {
    val m2OutputPath = "$outputDir/m2_temporal_demand"
    println("[M2] Writing partitioned output by pickup_borough to: $m2OutputPath")
    df.write()
        .mode(SaveMode.Overwrite)
        .partitionBy("pickup_borough")
        .parquet(m2OutputPath)
}
