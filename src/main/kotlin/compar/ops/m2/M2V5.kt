package compar.ops.m2

import compar.ops.Analysis
import compar.ops.AnalysisInput
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions as sf
import org.apache.spark.sql.types.DataTypes

/**
 * M2 v5 — O(1) array-indexed borough lookup and deferred duration division.
 * Bottleneck (v4): mapping PULocationID to boroughId via chained WHEN ... IN (...) clauses
 * evaluates dozens of comparisons per row across tens of millions of trips, and tripDurationMin()
 * calls unix_timestamp() (with timezone conversion) and divides by 60.0 on every input row.
 * Change: build a 263-element array indexed directly by PULocationID for O(1) lookup via element_at,
 * compute duration in seconds via direct Long cast, and divide by 60.0 only once per group after avg().
 */
class M2V5 : Analysis {

    override fun run(input: AnalysisInput): Dataset<Row> {
        println("\n--- [$tag] Temporal Demand Analysis ---")

        // Step 1: Build an O(1) lookup array indexed by PULocationID (1..263) -> boroughId (0..6)
        val (boroughs, boroughByZoneId) = buildZoneLookupArray(input.zones)
        val boroughDayHourKey = packBoroughDayHour(lookupBoroughId(boroughByZoneId))

        // Step 2: Single aggregation by the 11-bit key; divide mean duration by 60.0 only on the <= 1,176 groups
        val aggregated = aggregateBoroughDayHour(input.trips, boroughDayHourKey)

        // Step 3: Decode the 11-bit key back into (pickup_borough, day_of_week, hour_of_day)
        val m2Df = decodeBoroughDayHour(aggregated, boroughs)
            .repartition(sf.col(COL_PICKUP_BOROUGH))

        writeTemporalDemand(m2Df, input.outputDir)

        return m2Df
    }
}

/** Structured holder for driver-resolved zone lookup data. */
internal data class ZoneLookup(
    val boroughs: List<String>,
    val boroughByZoneId: Column
)

/** Resolves PULocationID (cast from BIGINT to INT for Spark element_at) to its 3-bit borough index (0..6). */
internal fun lookupBoroughId(boroughByZoneId: Column): Column =
    sf.element_at(boroughByZoneId, sf.col(COL_PU_LOCATION_ID).cast(DataTypes.IntegerType))

/** Transforms location-to-borough pairs into an O(1) Spark lookup array. */
internal fun createZoneLookup(zones: Iterable<Pair<Int, String>>): ZoneLookup {
    val boroughs = zones.map { it.second }.distinct().sorted()
    val boroughIndex = boroughs.withIndex().associate { (idx, name) -> name to idx }
    val unknownIndex = boroughs.size

    // Spark's element_at is 1-based, so index 0 in Kotlin array corresponds to LocationID = 1
    val lookupTable = IntArray(MAX_LOCATION_ID) { unknownIndex }
    for ((locationId, boroName) in zones) {
        if (locationId in 1..MAX_LOCATION_ID) {
            lookupTable[locationId - 1] = boroughIndex[boroName] ?: unknownIndex
        }
    }
    val sparkArray = sf.array(*lookupTable.map { sf.lit(it) }.toTypedArray())
    return ZoneLookup(boroughs, sparkArray)
}

/** Builds an O(1) array of borough indexes (0..6) indexed directly by LocationID. */
internal fun buildZoneLookupArray(zones: Dataset<Row>): ZoneLookup {
    val zoneRows = zones.select(COL_LOCATION_ID, COL_BOROUGH).collectAsList()
    val pairs = zoneRows.mapNotNull { row ->
        if (!row.isNullAt(ZONE_LOCATION_ID_IDX) && !row.isNullAt(ZONE_BOROUGH_IDX)) {
            row.getInt(ZONE_LOCATION_ID_IDX) to row.getString(ZONE_BOROUGH_IDX)
        } else null
    }
    return createZoneLookup(pairs)
}

/** Aggregates trips grouped by the 11-bit boroughDayHourKey with deferred duration division. */
internal fun aggregateBoroughDayHour(trips: Dataset<Row>, boroughDayHourKey: Column): Dataset<Row> = trips
    .select(*m2TripColumns)
    .groupBy(boroughDayHourKey.alias(COL_BOROUGH_DAY_HOUR_KEY))
    .agg(
        sf.count("*").alias(COL_TOTAL_TRIPS),
        sf.avg(COL_FARE_AMOUNT).alias(COL_MEAN_FARE_AMOUNT),
        sf.avg(tripDurationSeconds()).divide(60.0).alias(COL_MEAN_TRIP_DURATION_MIN),
        sf.avg(COL_TRIP_DISTANCE).alias(COL_MEAN_TRIP_DISTANCE)
    )

/** Decodes the 11-bit binary key back into standard columns (pickup_borough, day_of_week, hour_of_day). */
internal fun decodeBoroughDayHour(aggregated: Dataset<Row>, boroughs: List<String>): Dataset<Row> {
    val boroughNamesArray = sf.array(*(boroughs + UNKNOWN_BOROUGH).map { sf.lit(it) }.toTypedArray())
    // element_at is 1-based, so add 1 to the 0-based boroughId
    val boroughIdOneBased = sf.shiftrightunsigned(sf.col(COL_BOROUGH_DAY_HOUR_KEY), BOROUGH_SHIFT).plus(1)

    return aggregated
        .withColumn(COL_PICKUP_BOROUGH, sf.element_at(boroughNamesArray, boroughIdOneBased.cast(DataTypes.IntegerType)))
        .withColumn(COL_DAY_OF_WEEK, sf.shiftrightunsigned(sf.col(COL_BOROUGH_DAY_HOUR_KEY), DAY_SHIFT).bitwiseAND(DAY_MASK))
        .withColumn(COL_HOUR_OF_DAY, sf.col(COL_BOROUGH_DAY_HOUR_KEY).bitwiseAND(HOUR_MASK))
        .drop(COL_BOROUGH_DAY_HOUR_KEY)
}
