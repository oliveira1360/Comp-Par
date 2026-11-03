package compar.ops.o4

import compar.ops.Analysis
import compar.ops.AnalysisInput
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions as sf
import org.apache.spark.sql.types.DataTypes

// ============================================================================
// O4 v4 Constants
// ============================================================================

// Key dimension limits:
internal const val MAX_LOCATION_ID = 263
internal const val MAX_HOUR_OF_DAY = 23

// Why 8,440 array positions instead of dense 6,312 (263 * 24):
// - bucket_key uses bitshift (shl 5, i.e. * 32) because bitwise shifts and masks run in a single
//   CPU cycle and avoid runtime multiplication (* 24) and integer division/modulus (/ 24, % 24).
// - Each zone reserves 32 slots (2^5); hours 0..23 are used, while slots 24..31 per zone remain empty.
// - Max key is (263 shl 5) + 23 = (263 * 32) + 23 = 8,439.
// - Total array size is 8,439 + 1 = 8,440 elements.
//
// Array Element Representation Evolution (IntArray -> ByteArray):
// - Initially, an IntArray (32-bit per entry) was used, requiring ~33.7 KB.
// - However, zone_type_id only ranges from 0..3 (requiring strictly 2 bits).
// - We refined this to a ByteArray (8-bit ByteType), reducing array memory by 75% (~8.4 KB).
// - Why Byte instead of 2-bit packing inside Longs:
//   Packing 32 entries into a 64-bit Long (~2.1 KB) would require 4 extra bitwise operations
//   (shift + mask) on every single trip across 28.5M rows in Catalyst. A ByteArray hits the
//   optimal sweet spot: native 1-instruction O(1) indexing in CPU/L1 cache with a 75% memory saving.
internal const val MAX_BUCKET_KEY = (MAX_LOCATION_ID shl BUCKET_ZONE_SHIFT) + MAX_HOUR_OF_DAY // 8439
internal const val TOTAL_BUCKET_ARRAY_SIZE = MAX_BUCKET_KEY + 1 // 8440

internal const val COL_AIRPORT_TRIPS = "airport_trips"
internal const val COL_NEGOTIATED_TRIPS = "negotiated_trips"
internal const val COL_GROUP_TRIPS = "group_trips"

/**
 * O4 v4 — Integer bucket accumulators, complete join elimination via O(1) driver array lookup,
 * and coalesce(1) output write.
 *
 * Bottleneck (v3):
 * 1. Redundant Floating-Point Accumulators in Bucket Aggregation: classifyBucketsBitpacked() uses
 *    three separate sf.avg(when(..., 1.0).otherwise(0.0)) expressions alongside sf.count("*").
 *    Internally, each avg() maintains its own Double sum and redundant Long count (7 accumulators
 *    updated per row instead of 4 integer counters).
 * 2. BroadcastHashJoin Probing Overhead: Even with a 14-bit integer key, BroadcastHashJoin builds a
 *    HashedRelation and computes a 64-bit hash + hash table probe for every single trip (28.5M times).
 * 3. Fragmented Output Partitions: Writing <= 48 output rows without coalesce(1) produces multiple
 *    tiny Parquet part files.
 *
 * Change:
 * 1. Replaces the 3 floating-point avg() expressions in the bucket aggregation with 3 integer sum()
 *    counters, evaluating the fractions on the driver only for the <= 6,312 aggregated buckets.
 * 2. Builds a flat 8,440-element integer lookup array on the driver indexed directly by the 14-bit
 *    bucket_key, replacing BroadcastHashJoin with an O(1) element_at array lookup and fusing the
 *    lookup directly into aggregateByStatsKey().
 * 3. Applies coalesce(1) on the <= 48 final rows before writing to Parquet.
 */
class O4V4(
    private val airportThreshold: Double = DEFAULT_AIRPORT_THRESHOLD,
    private val negotiatedThreshold: Double = DEFAULT_NEGOTIATED_THRESHOLD,
    private val groupThreshold: Double = DEFAULT_GROUP_THRESHOLD,
    private val commercialMaxGroup: Double = DEFAULT_COMMERCIAL_MAX_GROUP
) : Analysis {

    override fun run(input: AnalysisInput): Dataset<Row> {
        println("\n--- [$tag] Trip-Type Classification ---")

        // Step 1: Pre-aggregate integer counts per 14-bit bucket_key and build O(1) lookup array on driver
        val zoneTypeLookupArray = buildDriverZoneTypeArray(
            trips = input.trips,
            airportThreshold = airportThreshold,
            negotiatedThreshold = negotiatedThreshold,
            groupThreshold = groupThreshold,
            commercialMaxGroup = commercialMaxGroup
        )

        // Step 2: Fused O(1) array lookup + stats_key packing + aggregation (zero joins!)
        val aggregated = aggregateFusedWithZoneTypeArray(input.trips, zoneTypeLookupArray)

        // Step 3: Decode Strings on <= 48 output rows (reused from v3) and coalesce(1)
        val o4Stats = decodeStatsKey(aggregated)
            .orderBy(COL_YEAR_MONTH, COL_ZONE_TYPE)
            .coalesce(1)

        writeTripTypeStats(o4Stats, input.outputDir)

        return o4Stats
    }
}

// ============================================================================
// Integer Bucket Aggregation & O(1) Driver Array Lookup (Introduced in v4)
// ============================================================================

/**
 * Aggregates bucket counts using only 4 integer accumulators (total, airport, negotiated, group)
 * instead of 3 floating-point avg() expressions, and constructs a flat 8,440-element Spark array
 * mapping bucket_key -> zone_type_id (0..3).
 */
internal fun buildDriverZoneTypeArray(
    trips: Dataset<Row>,
    airportThreshold: Double = DEFAULT_AIRPORT_THRESHOLD,
    negotiatedThreshold: Double = DEFAULT_NEGOTIATED_THRESHOLD,
    groupThreshold: Double = DEFAULT_GROUP_THRESHOLD,
    commercialMaxGroup: Double = DEFAULT_COMMERCIAL_MAX_GROUP
): Column {
    val bucketKeyCol = packBucketKey(sf.col(COL_PU_LOCATION_ID), sf.hour(sf.col(COL_PICKUP_DATETIME)))

    val bucketRows = trips
        .select(
            bucketKeyCol.alias(COL_BUCKET_KEY),
            sf.col(COL_RATECODE_ID),
            sf.col(COL_PASSENGER_COUNT)
        )
        .groupBy(COL_BUCKET_KEY)
        .agg(
            sf.count("*").alias(COL_BUCKET_TOTAL_TRIPS),
            sf.sum(sf.`when`(sf.col(COL_RATECODE_ID).isin(RATECODE_JFK, RATECODE_NEWARK), 1).otherwise(0))
                .alias(COL_AIRPORT_TRIPS),
            sf.sum(sf.`when`(sf.col(COL_RATECODE_ID).equalTo(RATECODE_NEGOTIATED), 1).otherwise(0))
                .alias(COL_NEGOTIATED_TRIPS),
            sf.sum(sf.`when`(sf.col(COL_PASSENGER_COUNT).geq(GROUP_MIN_PASSENGERS), 1).otherwise(0))
                .alias(COL_GROUP_TRIPS)
        )
        .collectAsList()

    val lookupTable = ByteArray(TOTAL_BUCKET_ARRAY_SIZE) { ZONE_TYPE_ID_MIXED.toByte() }
    for (row in bucketRows) {
        if (!row.isNullAt(0) && !row.isNullAt(1)) {
            val key = row.getAs<Number>(0).toInt()
            val total = row.getAs<Number>(1).toDouble()
            if (key in 0..MAX_BUCKET_KEY && total > 0.0) {
                val fracAirport = row.getAs<Number>(2).toDouble() / total
                val fracNegotiated = row.getAs<Number>(3).toDouble() / total
                val fracGroup = row.getAs<Number>(4).toDouble() / total

                lookupTable[key] = classifyZoneTypeIdScalar(
                    fracAirport = fracAirport,
                    fracNegotiated = fracNegotiated,
                    fracGroup = fracGroup,
                    airportThreshold = airportThreshold,
                    negotiatedThreshold = negotiatedThreshold,
                    groupThreshold = groupThreshold,
                    commercialMaxGroup = commercialMaxGroup
                ).toByte()
            }
        }
    }

    return sf.array(*lookupTable.map { sf.lit(it) }.toTypedArray())
}

/** Scalar classification rule on the driver matching classifyZoneTypeIdExpr(). */
internal fun classifyZoneTypeIdScalar(
    fracAirport: Double,
    fracNegotiated: Double,
    fracGroup: Double,
    airportThreshold: Double,
    negotiatedThreshold: Double,
    groupThreshold: Double,
    commercialMaxGroup: Double
): Int = when {
    fracAirport >= airportThreshold -> ZONE_TYPE_ID_TRANSIT
    fracGroup >= groupThreshold -> ZONE_TYPE_ID_RESIDENTIAL
    fracNegotiated >= negotiatedThreshold || fracGroup <= commercialMaxGroup -> ZONE_TYPE_ID_COMMERCIAL
    else -> ZONE_TYPE_ID_MIXED
}

/**
 * Resolves each trip's zone_type_id in O(1) time via element_at(zoneTypeLookupArray, bucket_key + 1)
 * and fuses it directly into the stats_key aggregation without any Join operator.
 */
internal fun aggregateFusedWithZoneTypeArray(
    trips: Dataset<Row>,
    zoneTypeLookupArray: Column
): Dataset<Row> {
    val bucketKeyExpr = packBucketKey(sf.col(COL_PU_LOCATION_ID), sf.hour(sf.col(COL_PICKUP_DATETIME)))
    val zoneTypeIdExpr = sf.element_at(
        zoneTypeLookupArray,
        bucketKeyExpr.plus(1).cast(DataTypes.IntegerType)
    )
    val statsKeyExpr = packStatsKey(sf.col(COL_PICKUP_DATETIME), zoneTypeIdExpr)

    val prunedTrips = trips.select(
        sf.col(COL_PU_LOCATION_ID),
        sf.col(COL_PICKUP_DATETIME),
        sf.col(COL_FARE_AMOUNT),
        sf.col(COL_TRIP_DISTANCE),
        sf.col(COL_TOTAL_AMOUNT)
    )

    return aggregateByStatsKey(prunedTrips, statsKeyExpr)
}
