package compar.ops.o4

import compar.ops.Analysis
import compar.ops.AnalysisInput
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions as sf

/**
 * O4 — Trip-Type Classification:
 * 1. Computes the airport, negotiated and group trip fractions per (PULocationID, hour_of_day) bucket.
 * 2. Classifies each bucket as 'residential', 'commercial', 'transit' or 'mixed'.
 * 3. Joins the classification back to the full trip table.
 * 4. Computes fare and distance statistics per zone type and month.
 */
class O4V0Baseline(
    private val airportThreshold: Double = 0.10,
    private val groupThreshold: Double = 0.08,
    private val commercialMaxGroup: Double = 0.04
) : Analysis {

    override fun run(input: AnalysisInput): Dataset<Row> {
        println("\n--- [$tag] Trip-Type Classification ---")

        val dfPrepared = input.trips
            .withColumn("hour_of_day", sf.hour(sf.col("tpep_pickup_datetime")))
            .withColumn("year_month", sf.date_format(sf.col("tpep_pickup_datetime"), "yyyy-MM"))

        // 1. Aggregate per (PULocationID, hour_of_day) bucket to compute the fractions
        val bucketFractions = dfPrepared
            .groupBy("PULocationID", "hour_of_day")
            .agg(
                sf.count("*").alias("bucket_total_trips"),
                sf.avg(sf.`when`(sf.col("RatecodeID").isin(2, 3), 1.0).otherwise(0.0)).alias("frac_airport"),
                sf.avg(sf.`when`(sf.col("RatecodeID").equalTo(5), 1.0).otherwise(0.0)).alias("frac_negotiated"),
                sf.avg(sf.`when`(sf.col("passenger_count").geq(4), 1.0).otherwise(0.0)).alias("frac_group")
            )

        // 2. Classify each bucket based on the configurable thresholds
        val zoneTypeExpr = sf.`when`(
            sf.col("frac_airport").geq(airportThreshold),
            sf.lit("transit")
        ).`when`(
            sf.col("frac_group").geq(groupThreshold),
            sf.lit("residential")
        ).`when`(
            sf.col("frac_group").leq(commercialMaxGroup),
            sf.lit("commercial")
        ).otherwise(
            sf.lit("mixed")
        )

        val classifiedBuckets = bucketFractions
            .withColumn("zone_type", zoneTypeExpr)
            .select("PULocationID", "hour_of_day", "zone_type")

        // 3. Join the classification back to the trip table
        val tripsWithZoneType = dfPrepared
            .join(classifiedBuckets, arrayOf("PULocationID", "hour_of_day"))

        // 4. Compute fare and distance statistics per zone type and month
        val o4Stats = tripsWithZoneType
            .groupBy("year_month", "zone_type")
            .agg(
                sf.count("*").alias("total_trips"),
                sf.avg("fare_amount").alias("mean_fare_amount"),
                sf.stddev("fare_amount").alias("stddev_fare_amount"),
                sf.avg("trip_distance").alias("mean_trip_distance"),
                sf.stddev("trip_distance").alias("stddev_trip_distance"),
                sf.avg("total_amount").alias("mean_total_amount")
            )
            .orderBy("year_month", "zone_type")

        o4Stats.show(20, false)

        return o4Stats
    }
}
