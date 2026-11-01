package compar.ops.m3

import compar.ops.Analysis
import compar.ops.AnalysisInput
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions as sf

/**
 * M3 — Zone-Level Statistics:
 * For each pickup zone and month, computes the mean and standard deviation of fare_amount and trip_distance.
 * Uses window functions to:
 * 1. Rank zones by monthly trip volume within each borough.
 * 2. Compute each zone's rolling 3-month trip share within its borough.
 * 3. Flag zones whose current-month volume deviates from their own rolling mean by more than 1.5 standard deviations.
 */
class M3V0Baseline : Analysis {

    override fun run(input: AnalysisInput): Dataset<Row> {
        println("\n--- [$tag] Zone-Level Statistics ---")

        val zonesPickup = input.zones.select(
            sf.col("LocationID").alias("PULocationID"),
            sf.col("Borough").alias("pickup_borough"),
            sf.col("Zone").alias("pickup_zone")
        )

        val monthlyZoneStats = input.trips
            .join(zonesPickup, "PULocationID")
            .withColumn("year_month", sf.date_format(sf.col("tpep_pickup_datetime"), "yyyy-MM"))
            .groupBy("PULocationID", "pickup_borough", "pickup_zone", "year_month")
            .agg(
                sf.count("*").alias("monthly_trip_volume"),
                sf.avg("fare_amount").alias("mean_fare_amount"),
                sf.stddev("fare_amount").alias("stddev_fare_amount"),
                sf.avg("trip_distance").alias("mean_trip_distance"),
                sf.stddev("trip_distance").alias("stddev_trip_distance")
            )

        val wBoroughMonthRank = Window
            .partitionBy("pickup_borough", "year_month")
            .orderBy(sf.col("monthly_trip_volume").desc())

        val wBoroughMonthTotal = Window
            .partitionBy("pickup_borough", "year_month")

        val wZoneRolling3M = Window
            .partitionBy("PULocationID")
            .orderBy("year_month")
            .rowsBetween(-2, 0)

        val m3Df = monthlyZoneStats
            .withColumn("borough_volume_rank", sf.rank().over(wBoroughMonthRank))
            .withColumn("borough_monthly_volume", sf.sum("monthly_trip_volume").over(wBoroughMonthTotal))
            .withColumn(
                "monthly_trip_share",
                sf.`when`(
                    sf.col("borough_monthly_volume").gt(0),
                    sf.col("monthly_trip_volume").divide(sf.col("borough_monthly_volume"))
                ).otherwise(0.0)
            )
            .withColumn("rolling_3m_trip_share", sf.avg("monthly_trip_share").over(wZoneRolling3M))
            .withColumn("rolling_mean_volume", sf.avg("monthly_trip_volume").over(wZoneRolling3M))
            .withColumn("rolling_stddev_volume", sf.stddev("monthly_trip_volume").over(wZoneRolling3M))
            .withColumn(
                "volume_deviation_flag",
                sf.coalesce(
                    sf.abs(sf.col("monthly_trip_volume").minus(sf.col("rolling_mean_volume")))
                        .gt(sf.col("rolling_stddev_volume").multiply(1.5)),
                    sf.lit(false)
                )
            )
            .drop("borough_monthly_volume", "monthly_trip_share")
            .orderBy(sf.col("year_month"), sf.col("pickup_borough"), sf.col("borough_volume_rank"))

        m3Df.show(20, false)

        val flaggedCount = m3Df.filter(sf.col("volume_deviation_flag").equalTo(true)).count()
        println(String.format("[$tag] Total zone-months with volume deviation > 1.5 std: %,d", flaggedCount))

        return m3Df
    }
}
