package compar.ops.m2

import compar.ops.Analysis
import compar.ops.AnalysisInput
import compar.ops.common.pickupZones
import compar.ops.common.tripDurationMin
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions as sf

/**
 * M2 — Temporal Demand Analysis (Baseline):
 * For each (pickup_borough, day_of_week, hour_of_day) triple, computes:
 * - total trip count (total_trips)
 * - mean fare_amount (mean_fare_amount)
 * - mean trip duration in minutes (mean_trip_duration_min)
 * - mean distance (mean_trip_distance)
 * Writes the result partitioned by pickup_borough in Parquet format.
 */
class M2V0Baseline : Analysis {

    override fun run(input: AnalysisInput): Dataset<Row> {
        println("\n--- [$tag] Temporal Demand Analysis ---")

        val m2Df = input.trips
            .join(pickupZones(input.zones), COL_PU_LOCATION_ID)
            .let(::aggregateTemporalDemand)
            .orderBy(COL_PICKUP_BOROUGH, COL_DAY_OF_WEEK, COL_HOUR_OF_DAY)

        writeTemporalDemand(m2Df, input.outputDir)

        return m2Df
    }
}

/** Standard baseline aggregation grouping by the 3 raw columns (includes UTF-8 String). */
internal fun aggregateTemporalDemand(df: Dataset<Row>): Dataset<Row> = df
    .withColumn(COL_DAY_OF_WEEK, sf.dayofweek(sf.col(COL_PICKUP_DATETIME)))
    .withColumn(COL_HOUR_OF_DAY, sf.hour(sf.col(COL_PICKUP_DATETIME)))
    .withColumn(COL_TRIP_DURATION_MIN, tripDurationMin())
    .groupBy(COL_PICKUP_BOROUGH, COL_DAY_OF_WEEK, COL_HOUR_OF_DAY)
    .agg(
        sf.count("*").alias(COL_TOTAL_TRIPS),
        sf.avg(COL_FARE_AMOUNT).alias(COL_MEAN_FARE_AMOUNT),
        sf.avg(COL_TRIP_DURATION_MIN).alias(COL_MEAN_TRIP_DURATION_MIN),
        sf.avg(COL_TRIP_DISTANCE).alias(COL_MEAN_TRIP_DISTANCE)
    )
