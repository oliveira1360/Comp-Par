package compar.ops.m2

import compar.ops.Analysis
import compar.ops.AnalysisInput
import compar.ops.common.pickupZones
import compar.ops.common.tripDurationMin
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions as sf

/**
 * M2 v3 — 17-bit key packing and post-aggregation join.
 * Bottleneck (v2): joining trips with the zone lookup table before grouping forces Spark to probe
 * the hash table for every single trip (millions of rows) and group by a multi-column key containing
 * a variable-length UTF-8 String (pickup_borough).
 * Change: pack (PULocationID [9 bits], day_of_week [3 bits], hour_of_day [5 bits]) into a single
 * 17-bit integer and pre-aggregate trips by this primitive integer first. This collapses millions of
 * trips into at most 263 * 7 * 24 = 44,184 rows before touching the zone lookup table, and only then
 * unpacks the bits, joins with the zones, and rolls up the sums into borough averages.
 */
class M2V3 : Analysis {

    override fun run(input: AnalysisInput): Dataset<Row> {
        println("\n--- [$tag] Temporal Demand Analysis ---")

        // Bit layout (17 bits total):
        // [ bits 16..8: PULocationID (1..263, 9 bits) | bits 7..5: day_of_week (1..7, 3 bits) | bits 4..0: hour_of_day (0..23, 5 bits) ]
        val dayOfWeek = sf.dayofweek(sf.col(COL_PICKUP_DATETIME))
        val hourOfDay = sf.hour(sf.col(COL_PICKUP_DATETIME))
        val zoneDayHourKey = sf.shiftleft(sf.col(COL_PU_LOCATION_ID), ZONE_SHIFT)
            .plus(sf.shiftleft(dayOfWeek, DAY_SHIFT))
            .plus(hourOfDay)

        // Step 1: Pre-aggregate millions of trips by the single 17-bit integer key (<= 44,184 groups)
        val tripsByZoneDayHour = input.trips
            .select(*m2TripColumns)
            .groupBy(zoneDayHourKey.alias(COL_ZONE_DAY_HOUR_KEY))
            .agg(
                sf.count("*").alias(COL_ZONE_TRIPS),
                sf.sum(COL_FARE_AMOUNT).alias(COL_SUM_FARE),
                sf.sum(tripDurationMin()).alias(COL_SUM_DURATION),
                sf.sum(COL_TRIP_DISTANCE).alias(COL_SUM_DISTANCE)
            )

        // Step 2: Unpack the 17-bit key on the small pre-aggregated table (<= 44,184 rows)
        val unpackedByZone = tripsByZoneDayHour
            .withColumn(COL_PU_LOCATION_ID, sf.shiftrightunsigned(sf.col(COL_ZONE_DAY_HOUR_KEY), ZONE_SHIFT))
            .withColumn(COL_DAY_OF_WEEK, sf.shiftrightunsigned(sf.col(COL_ZONE_DAY_HOUR_KEY), DAY_SHIFT).bitwiseAND(DAY_MASK))
            .withColumn(COL_HOUR_OF_DAY, sf.col(COL_ZONE_DAY_HOUR_KEY).bitwiseAND(HOUR_MASK))

        // Step 3: Join the small table with the zone lookup and roll up sums into borough means
        val totalTrips = sf.sum(COL_ZONE_TRIPS)
        val m2Df = unpackedByZone
            .join(sf.broadcast(pickupZones(input.zones)), COL_PU_LOCATION_ID)
            .groupBy(COL_PICKUP_BOROUGH, COL_DAY_OF_WEEK, COL_HOUR_OF_DAY)
            .agg(
                totalTrips.alias(COL_TOTAL_TRIPS),
                sf.sum(COL_SUM_FARE).divide(totalTrips).alias(COL_MEAN_FARE_AMOUNT),
                sf.sum(COL_SUM_DURATION).divide(totalTrips).alias(COL_MEAN_TRIP_DURATION_MIN),
                sf.sum(COL_SUM_DISTANCE).divide(totalTrips).alias(COL_MEAN_TRIP_DISTANCE)
            )
            .repartition(sf.col(COL_PICKUP_BOROUGH))

        writeTemporalDemand(m2Df, input.outputDir)

        return m2Df
    }
}
