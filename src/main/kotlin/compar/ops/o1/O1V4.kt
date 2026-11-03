package compar.ops.o1

import compar.ops.Analysis
import compar.ops.AnalysisInput
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions as sf

// ============================================================================
// Bitpacking Constants & Layouts for O1 v4
// ============================================================================

// 1. Bucket Key (14 bits total):
// [ bits 13..5: PULocationID (1..263, 9 bits) | bits 4..0: hour_of_day (0..23, 5 bits) ]
internal const val BUCKET_HOUR_BITS = 5
internal const val BUCKET_ZONE_SHIFT = BUCKET_HOUR_BITS

// 2. Rate Key (8 bits total):
// [ bits 7..5: borough_id (0..6, 3 bits) | bits 4..1: payment_type (1..6, 4 bits) | bit 0: is_weekend (0..1, 1 bit) ]
internal const val RATE_WEEKEND_BITS = 1
internal const val RATE_PAYMENT_BITS = 4
internal const val RATE_BOROUGH_BITS = 3

internal const val RATE_PAYMENT_SHIFT = RATE_WEEKEND_BITS
internal const val RATE_BOROUGH_SHIFT = RATE_WEEKEND_BITS + RATE_PAYMENT_BITS

internal const val RATE_WEEKEND_MASK = (1 shl RATE_WEEKEND_BITS) - 1 // 0b1 = 1
internal const val RATE_PAYMENT_MASK = (1 shl RATE_PAYMENT_BITS) - 1 // 0b1111 = 15
internal const val RATE_BOROUGH_MASK = (1 shl RATE_BOROUGH_BITS) - 1 // 0b111 = 7

internal const val COL_BUCKET_KEY = "bucket_key"
internal const val COL_RATE_KEY = "rate_key"
internal const val UNKNOWN_BOROUGH = "Unknown"

/**
 * O1 v4 — 14-bit bucket key packing and 8-bit rate key packing with post-aggregation decoding.
 *
 * Bottleneck (v3):
 * - Bucket aggregation and join operate on a compound key of 2 separate columns (PULocationID, hour_of_day).
 * - Joining the zone lookup table before aggregation forces Spark to carry String objects (pickup_borough)
 *   and compute String hashes for day_type across millions of trip records.
 *
 * Change:
 * - Packs (PULocationID [9 bits] + hour_of_day [5 bits]) into a single 14-bit integer bucket_key.
 *   Bucket grouping and broadcast join operate on a single primitive 32-bit Int.
 * - Resolves PULocationID -> borough_id (0..6, 3 bits) on the driver, eliminating the trips zone join.
 * - Packs (borough_id [3 bits] + payment_type [4 bits] + is_weekend [1 bit]) into a single 8-bit rate_key.
 *   The final groupBy condenses millions of trips into at most 84 groups using a single integer key.
 * - Decodes Strings (pickup_borough, day_type) ONLY on the <= 84 aggregated output rows.
 */
class O1V4 : Analysis {

    override fun run(input: AnalysisInput): Dataset<Row> {
        println("\n--- [$tag] Fare Anomaly Detection ---")

        // Step 1: Resolve zone lookup table once on driver: PULocationID -> boroughId (0..6)
        val (boroughs, zonesByBorough) = extractBoroughMapping(input.zones)
        val boroughIdExpr = buildBoroughIdExpr(boroughs, zonesByBorough)

        // Step 2: Project minimal columns and pack the 14-bit bucket_key (single project node)
        val tripsWithKeys = selectTripsWithBucketKey(input.trips)

        // Step 3: Compute bucket statistics grouped by the single 14-bit integer bucket_key
        val bucketStats = tripsWithKeys
            .groupBy(COL_BUCKET_KEY)
            .agg(
                sf.avg(COL_FARE_PER_KM).alias(COL_MEAN_FARE_PER_KM),
                sf.stddev(COL_FARE_PER_KM).alias(COL_STDDEV_FARE_PER_KM)
            )

        // Step 4: Broadcast join of bucket thresholds on the single 14-bit integer key
        val isAnomaly = anomalyCondition(
            fareCol = sf.col(COL_FARE_PER_KM),
            meanCol = sf.col(COL_MEAN_FARE_PER_KM),
            stddevCol = sf.col(COL_STDDEV_FARE_PER_KM)
        )
        val tripsFlagged = tripsWithKeys
            .join(sf.broadcast(bucketStats), COL_BUCKET_KEY)
            .withColumn(COL_IS_ANOMALY, isAnomaly)

        // Step 5: Pack the 8-bit rate_key and aggregate millions of trips by this single integer
        val rateKey = packRateKey(boroughIdExpr, sf.col(COL_PAYMENT_TYPE), sf.col(COL_PICKUP_DATETIME))
        val aggregated = aggregateByRateKey(tripsFlagged, rateKey)

        // Step 6: Decode the 8-bit rate_key back into Strings ONLY on the <= 84 output rows
        val anomalyRatesDf = decodeRateKey(aggregated, boroughs)
            .orderBy(COL_PICKUP_BOROUGH, COL_PAYMENT_TYPE, COL_DAY_TYPE)

        writeAnomalyRates(anomalyRatesDf, input.outputDir)

        return anomalyRatesDf
    }
}

// ============================================================================
// Bitpacking Helper Functions (Introduced in v4)
// ============================================================================

/** Packs (PULocationID [9 bits] + hour_of_day [5 bits]) into a single 14-bit integer. */
internal fun packBucketKey(puCol: Column, hourCol: Column): Column =
    sf.shiftleft(puCol, BUCKET_ZONE_SHIFT).plus(hourCol)

/** Projects minimal trip columns and computes the 14-bit bucket_key in a single pass. */
internal fun selectTripsWithBucketKey(trips: Dataset<Row>): Dataset<Row> {
    val bucketKeyExpr = packBucketKey(sf.col(COL_PU_LOCATION_ID), sf.hour(sf.col(COL_PICKUP_DATETIME)))
    return trips.select(
        sf.col(COL_PU_LOCATION_ID),
        sf.col(COL_PAYMENT_TYPE),
        sf.col(COL_PICKUP_DATETIME),
        bucketKeyExpr.alias(COL_BUCKET_KEY),
        farePerKm().alias(COL_FARE_PER_KM)
    )
}

/** Packs (borough_id [3 bits] + payment_type [4 bits] + is_weekend [1 bit]) into an 8-bit integer. */
internal fun packRateKey(boroughIdCol: Column, paymentTypeCol: Column, pickupDatetimeCol: Column): Column {
    val isWeekend = sf.`when`(sf.dayofweek(pickupDatetimeCol).isin(1, 7), 1).otherwise(0)
    return sf.shiftleft(boroughIdCol, RATE_BOROUGH_SHIFT)
        .plus(sf.shiftleft(paymentTypeCol, RATE_PAYMENT_SHIFT))
        .plus(isWeekend)
}

/** Aggregates trips grouped by the single 8-bit rate_key (<= 84 groups). */
internal fun aggregateByRateKey(tripsFlagged: Dataset<Row>, rateKey: Column): Dataset<Row> = tripsFlagged
    .groupBy(rateKey.alias(COL_RATE_KEY))
    .agg(
        sf.count("*").alias(COL_TOTAL_TRIPS),
        sf.sum(sf.`when`(sf.col(COL_IS_ANOMALY), 1).otherwise(0)).alias(COL_ANOMALY_TRIPS),
        sf.round(
            sf.avg(sf.`when`(sf.col(COL_IS_ANOMALY), 1.0).otherwise(0.0)).multiply(100.0),
            2
        ).alias(COL_ANOMALY_RATE_PCT)
    )

/** Decodes the 8-bit rate_key into (pickup_borough, payment_type, day_type) on the final aggregated rows. */
internal fun decodeRateKey(aggregated: Dataset<Row>, boroughs: List<String>): Dataset<Row> {
    val keyCol = sf.col(COL_RATE_KEY)
    val boroughIdCol = sf.shiftrightunsigned(keyCol, RATE_BOROUGH_SHIFT).bitwiseAND(RATE_BOROUGH_MASK)
    val paymentTypeCol = sf.shiftrightunsigned(keyCol, RATE_PAYMENT_SHIFT).bitwiseAND(RATE_PAYMENT_MASK)
    val isWeekendCol = keyCol.bitwiseAND(RATE_WEEKEND_MASK)

    // Map boroughId back to String
    var boroughNameExpr: Column? = null
    for ((index, borough) in boroughs.withIndex()) {
        val condition = boroughIdCol.equalTo(index)
        boroughNameExpr = if (boroughNameExpr == null) sf.`when`(condition, borough) else boroughNameExpr.`when`(condition, borough)
    }
    val finalBoroughExpr = boroughNameExpr?.otherwise(UNKNOWN_BOROUGH) ?: sf.lit(UNKNOWN_BOROUGH)

    val dayTypeResultExpr = sf.`when`(isWeekendCol.equalTo(1), DAY_TYPE_WEEKEND).otherwise(DAY_TYPE_WEEKDAY)

    return aggregated
        .withColumn(COL_PICKUP_BOROUGH, finalBoroughExpr)
        .withColumn(COL_PAYMENT_TYPE, paymentTypeCol)
        .withColumn(COL_DAY_TYPE, dayTypeResultExpr)
        .drop(COL_RATE_KEY)
}

/** Collects the 265-row zone table once on the driver and groups LocationIDs by Borough. */
internal fun extractBoroughMapping(zones: Dataset<Row>): Pair<List<String>, Map<String, List<Int>>> {
    val rows = zones.select("LocationID", "Borough").collectAsList()
    val zonesByBorough = HashMap<String, MutableList<Int>>(8)
    for (row in rows) {
        if (!row.isNullAt(0) && !row.isNullAt(1)) {
            val id = row.getAs<Number>(0).toInt()
            val boro = row.getString(1)
            zonesByBorough.getOrPut(boro) { ArrayList() }.add(id)
        }
    }
    val boroughs = zonesByBorough.keys.sorted()
    return Pair(boroughs, zonesByBorough)
}

/** Builds an expression mapping PULocationID to its 3-bit borough index (0..6). */
@Suppress("DuplicatedCode")
internal fun buildBoroughIdExpr(boroughs: List<String>, zonesByBorough: Map<String, List<Int>>): Column {
    val puCol = sf.col(COL_PU_LOCATION_ID)
    var expr: Column? = null
    for ((index, borough) in boroughs.withIndex()) {
        val ids = zonesByBorough[borough].orEmpty().toTypedArray()
        val condition = puCol.isin(*ids)
        expr = if (expr == null) sf.`when`(condition, index) else expr.`when`(condition, index)
    }
    return expr?.otherwise(boroughs.size) ?: sf.lit(0)
}

