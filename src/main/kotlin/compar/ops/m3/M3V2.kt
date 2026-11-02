package compar.ops.m3

import compar.ops.Analysis
import compar.ops.AnalysisInput
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions as sf

/**
 * M3 v2 — integer month key and aggregation before the join.
 * Bottleneck (v1): date_format builds a string for each of the 35M rows, and each row goes through
 * the join, carrying the borough and zone strings in the grouping key.
 * Change: rows are grouped by (PULocationID, year * 100 + month), both integers. The join and the
 * "yyyy-MM" string then run on about 3 thousand rows instead of 35 million.
 */
class M3V2 : Analysis {

    override fun run(input: AnalysisInput): Dataset<Row> {
        println("\n--- [$tag] Zone-Level Statistics ---")

        val monthKey = sf.col("month_key")
        val monthlyZoneStats = aggregateByZoneMonthKey(input.trips)
            .join(sf.broadcast(pickupZoneNames(input.zones)), "PULocationID")
            .withColumn("year_month", sf.format_string("%04d-%02d", sf.floor(monthKey.divide(100)), monthKey.mod(100)))
            .select(
                "PULocationID", "pickup_borough", "pickup_zone", "year_month", "monthly_trip_volume",
                "mean_fare_amount", "stddev_fare_amount", "mean_trip_distance", "stddev_trip_distance"
            )

        val m3Df = withZoneWindows(monthlyZoneStats).cache()

        m3Df.show(20, false)

        val flaggedCount = m3Df.filter(sf.col("volume_deviation_flag").equalTo(true)).count()
        println(String.format("[$tag] Total zone-months with volume deviation > 1.5 std: %,d", flaggedCount))

        return m3Df
    }
}
