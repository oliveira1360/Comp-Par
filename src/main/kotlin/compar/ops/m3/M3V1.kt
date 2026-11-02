package compar.ops.m3

import compar.ops.Analysis
import compar.ops.AnalysisInput
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions as sf

/**
 * M3 v1 — one scan and a broadcast join.
 * Bottleneck (v0): show() and count() are separate actions, so the trips are read and aggregated twice,
 * and the join may shuffle 35M rows even though the zone table has 265.
 * Change: the small final table (about 3 thousand rows) is cached, so the second action reuses it,
 * and the zone table is broadcast.
 */
class M3V1 : Analysis {

    override fun run(input: AnalysisInput): Dataset<Row> {
        println("\n--- [$tag] Zone-Level Statistics ---")

        val monthlyZoneStats = input.trips
            .join(sf.broadcast(pickupZoneNames(input.zones)), "PULocationID")
            .withColumn("year_month", sf.date_format(sf.col("tpep_pickup_datetime"), "yyyy-MM"))
            .groupBy("PULocationID", "pickup_borough", "pickup_zone", "year_month")
            .agg(
                sf.count("*").alias("monthly_trip_volume"),
                sf.avg("fare_amount").alias("mean_fare_amount"),
                sf.stddev("fare_amount").alias("stddev_fare_amount"),
                sf.avg("trip_distance").alias("mean_trip_distance"),
                sf.stddev("trip_distance").alias("stddev_trip_distance")
            )

        val m3Df = withZoneWindows(monthlyZoneStats).cache()

        m3Df.show(20, false)

        val flaggedCount = m3Df.filter(sf.col("volume_deviation_flag").equalTo(true)).count()
        println(String.format("[$tag] Total zone-months with volume deviation > 1.5 std: %,d", flaggedCount))

        return m3Df
    }
}
