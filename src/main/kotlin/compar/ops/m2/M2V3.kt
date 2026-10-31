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
        println("\n--- [M2] Análise de Procura Temporal (v3) ---")

        // Bit layout (17 bits total):
        // [ bits 16..8: PULocationID (1..263, 9 bits) | bits 7..5: day_of_week (1..7, 3 bits) | bits 4..0: hour_of_day (0..23, 5 bits) ]
        val dayOfWeek = sf.dayofweek(sf.col("tpep_pickup_datetime"))
        val hourOfDay = sf.hour(sf.col("tpep_pickup_datetime"))
        val zoneDayHourKey = sf.shiftleft(sf.col("PULocationID"), ZONE_SHIFT)
            .plus(sf.shiftleft(dayOfWeek, DAY_SHIFT))
            .plus(hourOfDay)

        // Step 1: Pre-aggregate millions of trips by the single 17-bit integer key (<= 44,184 groups)
        val tripsByZoneDayHour = input.trips
            .select(*m2TripColumns)
            .groupBy(zoneDayHourKey.alias("zone_day_hour_key"))
            .agg(
                sf.count("*").alias("zone_trips"),
                sf.sum("fare_amount").alias("sum_fare"),
                sf.sum(tripDurationMin()).alias("sum_duration"),
                sf.sum("trip_distance").alias("sum_distance")
            )

        // Step 2: Unpack the 17-bit key on the small pre-aggregated table (<= 44,184 rows)
        val unpackedByZone = tripsByZoneDayHour
            .withColumn("PULocationID", sf.shiftrightunsigned(sf.col("zone_day_hour_key"), ZONE_SHIFT))
            .withColumn("day_of_week", sf.shiftrightunsigned(sf.col("zone_day_hour_key"), DAY_SHIFT).bitwiseAND(DAY_MASK))
            .withColumn("hour_of_day", sf.col("zone_day_hour_key").bitwiseAND(HOUR_MASK))

        // Step 3: Join the small table with the zone lookup and roll up sums into borough means
        val totalTrips = sf.sum("zone_trips")
        val m2Df = unpackedByZone
            .join(sf.broadcast(pickupZones(input.zones)), "PULocationID")
            .groupBy("pickup_borough", "day_of_week", "hour_of_day")
            .agg(
                totalTrips.alias("total_trips"),
                sf.sum("sum_fare").divide(totalTrips).alias("mean_fare_amount"),
                sf.sum("sum_duration").divide(totalTrips).alias("mean_trip_duration_min"),
                sf.sum("sum_distance").divide(totalTrips).alias("mean_trip_distance")
            )
            .repartition(sf.col("pickup_borough"))

        writeTemporalDemand(m2Df, input.outputDir)

        return m2Df
    }
}

private const val HOUR_BITS = 5                                // 0..23 fits in 5 bits (max 31)
private const val DAY_BITS = 3                                 // 1..7 fits in 3 bits (max 7)
private const val DAY_SHIFT = HOUR_BITS                        // shift day_of_week by 5 bits
private const val ZONE_SHIFT = HOUR_BITS + DAY_BITS            // shift PULocationID by 8 bits
private const val HOUR_MASK = (1 shl HOUR_BITS) - 1            // 0b11111 = 31
private const val DAY_MASK = (1 shl DAY_BITS) - 1              // 0b111 = 7
