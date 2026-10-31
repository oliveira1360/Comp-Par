package compar.ops.m2

import compar.ops.Analysis
import compar.ops.AnalysisInput
import compar.ops.common.tripDurationMin
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions as sf

/**
 * M2 v4 — 11-bit borough key packing and join elimination.
 * Bottleneck (v3): packing PULocationID (263 zones) created up to 44,184 groups per task,
 * increasing shuffle volume to 13.6 MB and requiring a second groupBy after the broadcast join.
 * Change: resolve the 265-row zone lookup table once on the driver to map each PULocationID
 * directly to a 3-bit borough index (0..6). Pack (borough_id [3 bits], day_of_week [3 bits],
 * hour_of_day [5 bits]) into an 11-bit integer (at most 1,176 groups), eliminating both the
 * Spark join and the second aggregation stage.
 */
class M2V4 : Analysis {

    override fun run(input: AnalysisInput): Dataset<Row> {
        println("\n--- [M2] Análise de Procura Temporal (v4) ---")

        // Step 1: Collect the tiny 265-row zone table on the driver and group zone IDs by borough
        val zoneRows = input.zones.select("LocationID", "Borough").collectAsList()
        val zonesByBorough = zoneRows
            .filter { !it.isNullAt(0) && !it.isNullAt(1) }
            .groupBy({ it.getString(1) }, { it.getInt(0) })
        val boroughs = zonesByBorough.keys.sorted()

        // Map PULocationID -> boroughId (0..6, fits in 3 bits)
        val boroughIdExpr = buildBoroughIdExpr(boroughs, zonesByBorough)

        // Bit layout (11 bits total):
        // [ bits 10..8: boroughId (0..6, 3 bits) | bits 7..5: day_of_week (1..7, 3 bits) | bits 4..0: hour_of_day (0..23, 5 bits) ]
        val dayOfWeek = sf.dayofweek(sf.col("tpep_pickup_datetime"))
        val hourOfDay = sf.hour(sf.col("tpep_pickup_datetime"))
        val boroughDayHourKey = sf.shiftleft(boroughIdExpr, BOROUGH_SHIFT)
            .plus(sf.shiftleft(dayOfWeek, DAY_SHIFT))
            .plus(hourOfDay)

        // Step 2: Single aggregation by the 11-bit key (at most 7 * 7 * 24 = 1,176 groups, no join needed)
        val aggregated = input.trips
            .select(*m2TripColumns)
            .groupBy(boroughDayHourKey.alias("borough_day_hour_key"))
            .agg(
                sf.count("*").alias("total_trips"),
                sf.avg("fare_amount").alias("mean_fare_amount"),
                sf.avg(tripDurationMin()).alias("mean_trip_duration_min"),
                sf.avg("trip_distance").alias("mean_trip_distance")
            )

        // Step 3: Unpack the 11-bit key into (pickup_borough, day_of_week, hour_of_day)
        val boroughIdCol = sf.shiftrightunsigned(sf.col("borough_day_hour_key"), BOROUGH_SHIFT)
        val m2Df = aggregated
            .withColumn("pickup_borough", buildBoroughNameExpr(boroughs, boroughIdCol))
            .withColumn("day_of_week", sf.shiftrightunsigned(sf.col("borough_day_hour_key"), DAY_SHIFT).bitwiseAND(DAY_MASK))
            .withColumn("hour_of_day", sf.col("borough_day_hour_key").bitwiseAND(HOUR_MASK))
            .drop("borough_day_hour_key")
            .repartition(sf.col("pickup_borough"))

        writeTemporalDemand(m2Df, input.outputDir)

        return m2Df
    }

    /** Maps PULocationID to its 3-bit borough index (0..6). */
    private fun buildBoroughIdExpr(boroughs: List<String>, zonesByBorough: Map<String, List<Int>>): Column {
        val puCol = sf.col("PULocationID")
        var expr: Column? = null
        for ((index, borough) in boroughs.withIndex()) {
            val zoneIds = zonesByBorough[borough].orEmpty().toTypedArray()
            val condition = puCol.isin(*zoneIds)
            expr = if (expr == null) sf.`when`(condition, index) else expr.`when`(condition, index)
        }
        return expr?.otherwise(boroughs.size) ?: sf.lit(0)
    }

    /** Maps the 3-bit borough index (0..6) back to the borough name String on the <= 1,176 output rows. */
    private fun buildBoroughNameExpr(boroughs: List<String>, boroughIdCol: Column): Column {
        var expr: Column? = null
        for ((index, borough) in boroughs.withIndex()) {
            val condition = boroughIdCol.equalTo(index)
            expr = if (expr == null) sf.`when`(condition, borough) else expr.`when`(condition, borough)
        }
        return expr?.otherwise("Unknown") ?: sf.lit("Unknown")
    }
}

private const val HOUR_BITS = 5                                // 0..23 fits in 5 bits (max 31)
private const val DAY_BITS = 3                                 // 1..7 fits in 3 bits (max 7)
private const val DAY_SHIFT = HOUR_BITS                        // shift day_of_week by 5 bits
private const val BOROUGH_SHIFT = HOUR_BITS + DAY_BITS         // shift boroughId by 8 bits
private const val HOUR_MASK = (1 shl HOUR_BITS) - 1            // 0b11111 = 31
private const val DAY_MASK = (1 shl DAY_BITS) - 1              // 0b111 = 7
