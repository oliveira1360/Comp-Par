package compar.ops.m3

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions as sf


/** Zone table with the column names used by the M3 output. */
internal fun pickupZoneNames(zones: Dataset<Row>): Dataset<Row> = zones.select(
    sf.col("LocationID").alias("PULocationID"),
    sf.col("Borough").alias("pickup_borough"),
    sf.col("Zone").alias("pickup_zone")
)

/**
 * Adds the rank, the rolling 3-month share and the deviation flag to the (zone, month) statistics.
 * The share uses a window with the current month; the flag compares it with the previous 3 months, as in v0.
 */
internal fun withZoneWindows(monthlyZoneStats: Dataset<Row>): Dataset<Row> {
    val wBoroughMonthRank = Window
        .partitionBy("pickup_borough", "year_month")
        .orderBy(sf.col("monthly_trip_volume").desc())

    val wBoroughMonthTotal = Window
        .partitionBy("pickup_borough", "year_month")

    val wZoneRolling3M = Window
        .partitionBy("PULocationID")
        .orderBy("year_month")
        .rowsBetween(-2, 0)

    val wZonePrevious3M = Window
        .partitionBy("PULocationID")
        .orderBy("year_month")
        .rowsBetween(-3, -1)

    return monthlyZoneStats
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
        .withColumn("rolling_mean_volume", sf.avg("monthly_trip_volume").over(wZonePrevious3M))
        .withColumn("rolling_stddev_volume", sf.stddev("monthly_trip_volume").over(wZonePrevious3M))
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
}

/** Statistics per (zone, month) with an integer month key, aggregated before the zone names are joined. */
internal fun aggregateByZoneMonthKey(trips: Dataset<Row>): Dataset<Row> {
    val pickup = sf.col("tpep_pickup_datetime")
    return trips
        .groupBy(sf.col("PULocationID"), sf.year(pickup).multiply(100).plus(sf.month(pickup)).alias("month_key"))
        .agg(
            sf.count("*").alias("monthly_trip_volume"),
            sf.avg("fare_amount").alias("mean_fare_amount"),
            sf.stddev("fare_amount").alias("stddev_fare_amount"),
            sf.avg("trip_distance").alias("mean_trip_distance"),
            sf.stddev("trip_distance").alias("stddev_trip_distance")
        )
}
