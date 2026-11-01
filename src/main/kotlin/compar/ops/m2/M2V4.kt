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
        println("\n--- [$tag] Temporal Demand Analysis ---")

        // Step 1: Collect the 265-row zone table and group zone IDs by borough using sequences
        val zoneRows = input.zones.select(COL_LOCATION_ID, COL_BOROUGH).collectAsList()
        val zonesByBorough = HashMap<String, MutableList<Int>>(8)
        zoneRows.asSequence()
            .filter { !it.isNullAt(ZONE_LOCATION_ID_IDX) && !it.isNullAt(ZONE_BOROUGH_IDX) }
            .forEach { row ->
                zonesByBorough.getOrPut(row.getString(ZONE_BOROUGH_IDX)) { ArrayList() }
                    .add(row.getInt(ZONE_LOCATION_ID_IDX))
            }
        val boroughs = zonesByBorough.keys.sorted()

        // Map PULocationID -> boroughId (0..6, fits in 3 bits)
        val boroughIdExpr = buildBoroughIdExpr(boroughs, zonesByBorough)
        val boroughDayHourKey = packBoroughDayHour(boroughIdExpr)

        // Step 2: Single aggregation by the 11-bit key (at most 7 * 7 * 24 = 1,176 groups, no join needed)
        val aggregated = input.trips
            .select(*m2TripColumns)
            .groupBy(boroughDayHourKey.alias(COL_BOROUGH_DAY_HOUR_KEY))
            .agg(
                sf.count("*").alias(COL_TOTAL_TRIPS),
                sf.avg(COL_FARE_AMOUNT).alias(COL_MEAN_FARE_AMOUNT),
                sf.avg(tripDurationMin()).alias(COL_MEAN_TRIP_DURATION_MIN),
                sf.avg(COL_TRIP_DISTANCE).alias(COL_MEAN_TRIP_DISTANCE)
            )

        // Step 3: Unpack the 11-bit key into (pickup_borough, day_of_week, hour_of_day)
        val boroughIdCol = sf.shiftrightunsigned(sf.col(COL_BOROUGH_DAY_HOUR_KEY), BOROUGH_SHIFT)
        val m2Df = aggregated
            .withColumn(COL_PICKUP_BOROUGH, buildBoroughNameExpr(boroughs, boroughIdCol))
            .withColumn(COL_DAY_OF_WEEK, sf.shiftrightunsigned(sf.col(COL_BOROUGH_DAY_HOUR_KEY), DAY_SHIFT).bitwiseAND(DAY_MASK))
            .withColumn(COL_HOUR_OF_DAY, sf.col(COL_BOROUGH_DAY_HOUR_KEY).bitwiseAND(HOUR_MASK))
            .drop(COL_BOROUGH_DAY_HOUR_KEY)
            .repartition(sf.col(COL_PICKUP_BOROUGH))

        writeTemporalDemand(m2Df, input.outputDir)

        return m2Df
    }

    /** Maps PULocationID to its 3-bit borough index (0..6). */
    private fun buildBoroughIdExpr(boroughs: List<String>, zonesByBorough: Map<String, List<Int>>): Column {
        val puCol = sf.col(COL_PU_LOCATION_ID)
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
        return expr?.otherwise(UNKNOWN_BOROUGH) ?: sf.lit(UNKNOWN_BOROUGH)
    }
}

/**
 * Bit layout (11 bits total):
 * [ bits 10..8: boroughId (0..6, 3 bits) | bits 7..5: day_of_week (1..7, 3 bits) | bits 4..0: hour_of_day (0..23, 5 bits) ]
 */
internal fun packBoroughDayHour(boroughIdExpr: Column): Column {
    val dayOfWeek = sf.dayofweek(sf.col(COL_PICKUP_DATETIME))
    val hourOfDay = sf.hour(sf.col(COL_PICKUP_DATETIME))
    return sf.shiftleft(boroughIdExpr, BOROUGH_SHIFT)
        .plus(sf.shiftleft(dayOfWeek, DAY_SHIFT))
        .plus(hourOfDay)
}
