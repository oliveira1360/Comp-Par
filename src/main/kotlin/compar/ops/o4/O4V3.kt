    package compar.ops.o4

import compar.ops.Analysis
import compar.ops.AnalysisInput
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions as sf

// ============================================================================
// Bitpacking Constants & Layouts for O4 v3
// ============================================================================

// 1. Bucket Key (14 bits total):
// [ bits 13..5: PULocationID (1..263, 9 bits) | bits 4..0: hour_of_day (0..23, 5 bits) ]
//
// Why 5 bits (32 slots per zone) instead of multiplying by 24:
// - hour_of_day (0..23) requires 5 bits (2^5 = 32 values; 4 bits only hold 0..15).
// - Using a bitshift (shl 5) maps to a single-cycle hardware bitwise instruction in the CPU,
//   avoiding arithmetic multiplication (* 24) and costly integer division/modulo (/ 24 and % 24)
//   during bit manipulation.
// - Trade-off: Each zone gets 32 slots instead of 24 (slots 24..31 per zone remain unused).
//   Dense indexing would yield 263 * 24 = 6,312 slots, while bit-aligned indexing yields
//   up to (263 * 32) + 23 = 8,439 (8,440 slots). The difference in memory is only ~8 KB,
//   which is negligible compared to the execution speedup of native bitwise operations.
internal const val BUCKET_HOUR_BITS = 5
internal const val BUCKET_ZONE_SHIFT = BUCKET_HOUR_BITS

// 2. Zone Type ID (2 bits: 0..3):
internal const val ZONE_TYPE_ID_TRANSIT = 0
internal const val ZONE_TYPE_ID_RESIDENTIAL = 1
internal const val ZONE_TYPE_ID_COMMERCIAL = 2
internal const val ZONE_TYPE_ID_MIXED = 3

// 3. Stats Key (packs year_month_int = year * 100 + month and 2-bit zone_type_id):
// [ bits 20..2: year_month_int (e.g. 201501) | bits 1..0: zone_type_id (0..3, 2 bits) ]
internal const val STATS_ZONE_TYPE_BITS = 2
internal const val STATS_MONTH_SHIFT = STATS_ZONE_TYPE_BITS
internal const val STATS_ZONE_TYPE_MASK = (1 shl STATS_ZONE_TYPE_BITS) - 1 // 0b11 = 3

internal const val COL_BUCKET_KEY = "bucket_key"
internal const val COL_ZONE_TYPE_ID = "zone_type_id"
internal const val COL_STATS_KEY = "stats_key"

/**
 * O4 v3 — 14-bit bucket key packing, 2-bit zone_type_id encoding, and packed integer stats_key
 * with post-aggregation String decoding.
 *
 * Bottleneck (v2):
 * - Bucket aggregation and BroadcastHashJoin operate on a compound 2-column key (PULocationID, hour_of_day)
 *   and carry variable-length UTF8String values ("transit", "residential", "commercial", "mixed").
 * - The second pass evaluates date_format(tpep_pickup_datetime, "yyyy-MM") on every single trip
 *   (28.5M String allocations) and groups by two String columns (year_month, zone_type).
 *
 * Change:
 * - Packs (PULocationID [9 bits] | hour_of_day [5 bits]) into a single 14-bit integer bucket_key.
 * - Encodes zone_type as a 2-bit integer zone_type_id (0..3) during bucket classification and broadcast join.
 * - Packs ((year * 100 + month) << 2 | zone_type_id) into a single primitive integer stats_key for the
 *   final groupBy, completely eliminating String allocations and String hashing across the 28.5M trips.
 * - Decodes year_month ("yyyy-MM") and zone_type Strings ONLY on the <= 48 final aggregated rows.
 */
class O4V3(
    private val airportThreshold: Double = DEFAULT_AIRPORT_THRESHOLD,
    private val negotiatedThreshold: Double = DEFAULT_NEGOTIATED_THRESHOLD,
    private val groupThreshold: Double = DEFAULT_GROUP_THRESHOLD,
    private val commercialMaxGroup: Double = DEFAULT_COMMERCIAL_MAX_GROUP
) : Analysis {

    override fun run(input: AnalysisInput): Dataset<Row> {
        println("\n--- [$tag] Trip-Type Classification ---")

        // Step 1: Project minimal bucket columns with 14-bit packed bucket_key
        val dfBucketInput = selectTripsWithBucketKey(input.trips)

        // Step 2: Aggregate per 14-bit bucket_key and classify into a 2-bit integer zone_type_id (0..3)
        val classifiedBuckets = classifyBucketsBitpacked(
            dfBucketInput = dfBucketInput,
            airportThreshold = airportThreshold,
            negotiatedThreshold = negotiatedThreshold,
            groupThreshold = groupThreshold,
            commercialMaxGroup = commercialMaxGroup
        )

        // Step 3: Project stats columns with 14-bit bucket_key and Broadcast Join on the single Int key
        val dfStatsInput = selectStatsWithBucketKey(input.trips)
        val tripsWithZoneTypeId = dfStatsInput
            .join(sf.broadcast(classifiedBuckets), COL_BUCKET_KEY)

        // Step 4: Pack (year_month_int << 2 | zone_type_id) into a single integer stats_key and aggregate
        val statsKeyExpr = packStatsKey(sf.col(COL_PICKUP_DATETIME), sf.col(COL_ZONE_TYPE_ID))
        val aggregated = aggregateByStatsKey(tripsWithZoneTypeId, statsKeyExpr)

        // Step 5: Decode Strings (year_month, zone_type) ONLY on the <= 48 final aggregated rows
        val o4Stats = decodeStatsKey(aggregated)
            .orderBy(COL_YEAR_MONTH, COL_ZONE_TYPE)

        writeTripTypeStats(o4Stats, input.outputDir)

        return o4Stats
    }
}

// ============================================================================
// Bitpacking Helper Functions (Introduced in v3)
// ============================================================================

/** Packs (PULocationID [9 bits] + hour_of_day [5 bits]) into a single 14-bit integer. */
internal fun packBucketKey(puCol: Column, hourCol: Column): Column =
    sf.shiftleft(puCol, BUCKET_ZONE_SHIFT).plus(hourCol)

/** Projects only the 3 columns needed for bucket classification with the 14-bit bucket_key. */
internal fun selectTripsWithBucketKey(trips: Dataset<Row>): Dataset<Row> {
    val bucketKeyExpr = packBucketKey(sf.col(COL_PU_LOCATION_ID), sf.hour(sf.col(COL_PICKUP_DATETIME)))
    return trips.select(
        bucketKeyExpr.alias(COL_BUCKET_KEY),
        sf.col(COL_RATECODE_ID),
        sf.col(COL_PASSENGER_COUNT)
    )
}

/** Projects only the columns needed for the monthly stats pass with the 14-bit bucket_key. */
internal fun selectStatsWithBucketKey(trips: Dataset<Row>): Dataset<Row> {
    val bucketKeyExpr = packBucketKey(sf.col(COL_PU_LOCATION_ID), sf.hour(sf.col(COL_PICKUP_DATETIME)))
    return trips.select(
        bucketKeyExpr.alias(COL_BUCKET_KEY),
        sf.col(COL_PICKUP_DATETIME),
        sf.col(COL_FARE_AMOUNT),
        sf.col(COL_TRIP_DISTANCE),
        sf.col(COL_TOTAL_AMOUNT)
    )
}

/**
 * Classifies each 14-bit bucket_key into a 2-bit integer zone_type_id (0..3):
 * 0 = transit, 1 = residential, 2 = commercial, 3 = mixed.
 */
internal fun classifyZoneTypeIdExpr(
    fracAirportCol: Column = sf.col(COL_FRAC_AIRPORT),
    fracNegotiatedCol: Column = sf.col(COL_FRAC_NEGOTIATED),
    fracGroupCol: Column = sf.col(COL_FRAC_GROUP),
    airportThreshold: Double = DEFAULT_AIRPORT_THRESHOLD,
    negotiatedThreshold: Double = DEFAULT_NEGOTIATED_THRESHOLD,
    groupThreshold: Double = DEFAULT_GROUP_THRESHOLD,
    commercialMaxGroup: Double = DEFAULT_COMMERCIAL_MAX_GROUP
): Column = sf.`when`(
    fracAirportCol.geq(airportThreshold),
    sf.lit(ZONE_TYPE_ID_TRANSIT)
).`when`(
    fracGroupCol.geq(groupThreshold),
    sf.lit(ZONE_TYPE_ID_RESIDENTIAL)
).`when`(
    fracNegotiatedCol.geq(negotiatedThreshold).or(fracGroupCol.leq(commercialMaxGroup)),
    sf.lit(ZONE_TYPE_ID_COMMERCIAL)
).otherwise(
    sf.lit(ZONE_TYPE_ID_MIXED)
)

/** Aggregates by the single 14-bit bucket_key and returns (bucket_key, zone_type_id). */
internal fun classifyBucketsBitpacked(
    dfBucketInput: Dataset<Row>,
    airportThreshold: Double = DEFAULT_AIRPORT_THRESHOLD,
    negotiatedThreshold: Double = DEFAULT_NEGOTIATED_THRESHOLD,
    groupThreshold: Double = DEFAULT_GROUP_THRESHOLD,
    commercialMaxGroup: Double = DEFAULT_COMMERCIAL_MAX_GROUP
): Dataset<Row> {
    val bucketFractions = dfBucketInput
        .groupBy(COL_BUCKET_KEY)
        .agg(
            sf.count("*").alias(COL_BUCKET_TOTAL_TRIPS),
            sf.avg(sf.`when`(sf.col(COL_RATECODE_ID).isin(RATECODE_JFK, RATECODE_NEWARK), 1.0).otherwise(0.0))
                .alias(COL_FRAC_AIRPORT),
            sf.avg(sf.`when`(sf.col(COL_RATECODE_ID).equalTo(RATECODE_NEGOTIATED), 1.0).otherwise(0.0))
                .alias(COL_FRAC_NEGOTIATED),
            sf.avg(sf.`when`(sf.col(COL_PASSENGER_COUNT).geq(GROUP_MIN_PASSENGERS), 1.0).otherwise(0.0))
                .alias(COL_FRAC_GROUP)
        )

    val zoneTypeIdExpr = classifyZoneTypeIdExpr(
        airportThreshold = airportThreshold,
        negotiatedThreshold = negotiatedThreshold,
        groupThreshold = groupThreshold,
        commercialMaxGroup = commercialMaxGroup
    )

    return bucketFractions
        .withColumn(COL_ZONE_TYPE_ID, zoneTypeIdExpr)
        .select(COL_BUCKET_KEY, COL_ZONE_TYPE_ID)
}

/** Packs ((year * 100 + month) << 2 | zone_type_id) into a single integer stats_key. */
internal fun packStatsKey(pickupDatetimeCol: Column, zoneTypeIdCol: Column): Column {
    val yearMonthInt = sf.year(pickupDatetimeCol).multiply(100).plus(sf.month(pickupDatetimeCol))
    return sf.shiftleft(yearMonthInt, STATS_MONTH_SHIFT).plus(zoneTypeIdCol)
}

/** Aggregates trip fare and distance metrics grouped by the single integer stats_key. */
internal fun aggregateByStatsKey(tripsDf: Dataset<Row>, statsKeyExpr: Column): Dataset<Row> = tripsDf
    .groupBy(statsKeyExpr.alias(COL_STATS_KEY))
    .agg(
        sf.count("*").alias(COL_TOTAL_TRIPS),
        sf.avg(COL_FARE_AMOUNT).alias(COL_MEAN_FARE_AMOUNT),
        sf.stddev(COL_FARE_AMOUNT).alias(COL_STDDEV_FARE_AMOUNT),
        sf.avg(COL_TRIP_DISTANCE).alias(COL_MEAN_TRIP_DISTANCE),
        sf.stddev(COL_TRIP_DISTANCE).alias(COL_STDDEV_TRIP_DISTANCE),
        sf.avg(COL_TOTAL_AMOUNT).alias(COL_MEAN_TOTAL_AMOUNT)
    )

/** Decodes stats_key back into (year_month, zone_type) Strings ONLY on the <= 48 aggregated output rows. */
internal fun decodeStatsKey(aggregated: Dataset<Row>): Dataset<Row> {
    val keyCol = sf.col(COL_STATS_KEY)
    val yearMonthInt = sf.shiftrightunsigned(keyCol, STATS_MONTH_SHIFT)
    val zoneTypeId = keyCol.bitwiseAND(STATS_ZONE_TYPE_MASK)

    val yearMonthStr = sf.format_string(
        "%04d-%02d",
        yearMonthInt.divide(100).cast("int"),
        yearMonthInt.mod(100)
    )

    val zoneTypeStr = sf.`when`(zoneTypeId.equalTo(ZONE_TYPE_ID_TRANSIT), ZONE_TYPE_TRANSIT)
        .`when`(zoneTypeId.equalTo(ZONE_TYPE_ID_RESIDENTIAL), ZONE_TYPE_RESIDENTIAL)
        .`when`(zoneTypeId.equalTo(ZONE_TYPE_ID_COMMERCIAL), ZONE_TYPE_COMMERCIAL)
        .otherwise(ZONE_TYPE_MIXED)

    return aggregated
        .select(
            yearMonthStr.alias(COL_YEAR_MONTH),
            zoneTypeStr.alias(COL_ZONE_TYPE),
            sf.col(COL_TOTAL_TRIPS),
            sf.col(COL_MEAN_FARE_AMOUNT),
            sf.col(COL_STDDEV_FARE_AMOUNT),
            sf.col(COL_MEAN_TRIP_DISTANCE),
            sf.col(COL_STDDEV_TRIP_DISTANCE),
            sf.col(COL_MEAN_TOTAL_AMOUNT)
        )
}
