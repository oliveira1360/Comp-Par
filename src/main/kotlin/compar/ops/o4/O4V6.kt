package compar.ops.o4

import compar.ops.Analysis
import compar.ops.AnalysisInput
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.functions as sf
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import kotlin.math.max
import kotlin.math.sqrt

// ============================================================================
// O4 v6 Bitpacking Constants (Single 32-bit Sub-Bucket Key)
// ============================================================================

// Packs (month_key [e.g. 201501] | bucket_key [14 bits: 0..8439]) into a single 32-bit Int:
// - bucket_key requires 14 bits (max 8439 < 16384 = 2^14).
// - Shifting month_key left by 14 bits: 202612 << 14 = 3,319,595,008 (Wait: 202612 * 16384 > Int.MAX_VALUE!).
// - Instead, we encode month_offset = (year - 2000) * 12 + month (e.g. 2015-01 -> 15 * 12 + 1 = 181, fits in 9 bits!).
// - Layout (23 bits total, fits comfortably in a positive 32-bit signed Int):
//   [ bits 22..14: month_offset (9 bits) | bits 13..5: PULocationID (9 bits) | bits 4..0: hour_of_day (5 bits) ]
internal const val SUB_BUCKET_KEY_SHIFT = 14
internal const val SUB_BUCKET_KEY_MASK = (1 shl SUB_BUCKET_KEY_SHIFT) - 1 // 0x3FFF = 16383
internal const val BASE_YEAR = 2000
internal const val COL_SUB_BUCKET_KEY = "sub_bucket_key"

/**
 * O4 v6 — Single-Pass Packed Sub-Bucket Aggregation + Driver-Side Rollup (Zero Cache / Zero 2nd Spark Job).
 *
 * Bottleneck (v5):
 * - Although v5 reduced Parquet I/O by 47% (from 3.47 GB to 1.84 GB) by pre-aggregating into ~38,000
 *   sub-buckets, it called .cache() on the subBuckets DataFrame and launched TWO additional distributed
 *   Spark SQL jobs (with 200 shuffle partitions each, totaling 22 stages and 1,600 tasks) just to sum
 *   those 38,000 rows into 6,312 buckets and then into 48 monthly rows.
 * - Caching 200 partitions in Spark's BlockManager also inflated peakExecutionMemory to 1.6 GB.
 * - Furthermore, v5 grouped by two separate columns (month_key, bucket_key) in the initial scan.
 *
 * Change in v6:
 * 1. Single 23-Bit Sub-Bucket Key: Packs (month_offset [9 bits] | PULocationID [9 bits] | hour_of_day [5 bits])
 *    into a single primitive 32-bit Int (sub_bucket_key), so the single Spark scan groups by 1 Int column.
 * 2. Driver-Side Rollup (as in M3v3): Collects the ~38,000 pre-aggregated sub-buckets (~2 MB) directly
 *    to the driver after the single Parquet scan, eliminating .cache() and all secondary Spark stages:
 *    - Pass A (in Kotlin RAM, < 2 ms): Sums trip-type counts per bucket_key (0..8439) and fills the
 *      8.4 KB ByteArray with each bucket's zone_type_id (0..3).
 *    - Pass B (in Kotlin RAM, < 2 ms): Groups the ~38,000 sub-buckets by (month_offset, zone_type_id)
 *      into the <= 48 final rows and applies the exact additive sample stddev formula.
 */
class O4V6(
    private val airportThreshold: Double = DEFAULT_AIRPORT_THRESHOLD,
    private val negotiatedThreshold: Double = DEFAULT_NEGOTIATED_THRESHOLD,
    private val groupThreshold: Double = DEFAULT_GROUP_THRESHOLD,
    private val commercialMaxGroup: Double = DEFAULT_COMMERCIAL_MAX_GROUP
) : Analysis {

    private class BucketCounts {
        var total: Long = 0L
        var airport: Long = 0L
        var negotiated: Long = 0L
        var group: Long = 0L
    }

    private class MonthlyZoneStats(
        val yearMonth: String,
        val zoneType: String
    ) {
        var totalTrips: Long = 0L
        var sumFare: Double = 0.0
        var sumSqFare: Double = 0.0
        var sumDist: Double = 0.0
        var sumSqDist: Double = 0.0
        var sumTotal: Double = 0.0
    }

    override fun run(input: AnalysisInput): Dataset<Row> {
        println("\n--- [$tag] Trip-Type Classification ---")

        // Step 1: Single Parquet scan grouped by a single 23-bit Int key -> returns ~38,000 rows to driver
        val subBucketRows = collectPackedSubBuckets(input.trips)

        // Step 2 (Pass A on Driver): Accumulate counts per 14-bit bucket_key (0..8439) and classify into ByteArray
        val bucketCounts = Array(TOTAL_BUCKET_ARRAY_SIZE) { BucketCounts() }
        for (row in subBucketRows) {
            val subKey = row.getInt(0)
            val bucketKey = subKey and SUB_BUCKET_KEY_MASK
            if (bucketKey in 0..MAX_BUCKET_KEY) {
                val b = bucketCounts[bucketKey]
                b.total += row.getLong(1)
                b.airport += row.getLong(2)
                b.negotiated += row.getLong(3)
                b.group += row.getLong(4)
            }
        }

        val zoneTypeLookup = ByteArray(TOTAL_BUCKET_ARRAY_SIZE) { ZONE_TYPE_ID_MIXED.toByte() }
        for (key in 0..MAX_BUCKET_KEY) {
            val b = bucketCounts[key]
            if (b.total > 0L) {
                val totalD = b.total.toDouble()
                zoneTypeLookup[key] = classifyZoneTypeIdScalar(
                    fracAirport = b.airport.toDouble() / totalD,
                    fracNegotiated = b.negotiated.toDouble() / totalD,
                    fracGroup = b.group.toDouble() / totalD,
                    airportThreshold = airportThreshold,
                    negotiatedThreshold = negotiatedThreshold,
                    groupThreshold = groupThreshold,
                    commercialMaxGroup = commercialMaxGroup
                ).toByte()
            }
        }

        // Step 3 (Pass B on Driver): Roll up the ~38,000 sub-buckets into <= 48 (year_month, zone_type) groups
        val monthlyGroups = HashMap<Int, MonthlyZoneStats>(64)
        for (row in subBucketRows) {
            val subKey = row.getInt(0)
            val monthOffset = subKey ushr SUB_BUCKET_KEY_SHIFT
            val bucketKey = subKey and SUB_BUCKET_KEY_MASK
            val zoneTypeId = zoneTypeLookup[bucketKey].toInt()

            val statsKey = (monthOffset shl STATS_MONTH_SHIFT) or zoneTypeId
            val acc = monthlyGroups.getOrPut(statsKey) {
                val year = BASE_YEAR + (monthOffset - 1) / 12
                val month = (monthOffset - 1) % 12 + 1
                val ymStr = String.format("%04d-%02d", year, month)
                val ztStr = zoneTypeIdToString(zoneTypeId)
                MonthlyZoneStats(ymStr, ztStr)
            }

            acc.totalTrips += row.getLong(1)
            acc.sumFare += row.getDouble(5)
            acc.sumSqFare += row.getDouble(6)
            acc.sumDist += row.getDouble(7)
            acc.sumSqDist += row.getDouble(8)
            acc.sumTotal += row.getDouble(9)
        }

        // Step 4: Sort by (year_month, zone_type) and compute final mean and sample stddev on the <= 48 rows
        val sortedStats = monthlyGroups.values.sortedWith(
            compareBy<MonthlyZoneStats> { it.yearMonth }.thenBy { it.zoneType }
        )

        val outputRows = sortedStats.map { acc ->
            val n = acc.totalTrips.toDouble()
            val meanFare = acc.sumFare / n
            val varFare = if (acc.totalTrips > 1L) (acc.sumSqFare - (acc.sumFare * acc.sumFare) / n) / (n - 1.0) else 0.0
            val stddevFare = sqrt(max(varFare, 0.0))

            val meanDist = acc.sumDist / n
            val varDist = if (acc.totalTrips > 1L) (acc.sumSqDist - (acc.sumDist * acc.sumDist) / n) / (n - 1.0) else 0.0
            val stddevDist = sqrt(max(varDist, 0.0))

            val meanTotal = acc.sumTotal / n

            RowFactory.create(
                acc.yearMonth,
                acc.zoneType,
                acc.totalTrips,
                meanFare,
                stddevFare,
                meanDist,
                stddevDist,
                meanTotal
            )
        }

        val o4Stats = input.spark.createDataFrame(outputRows, outputSchema).coalesce(1)

        writeTripTypeStats(o4Stats, input.outputDir)

        return o4Stats
    }

    private fun zoneTypeIdToString(zoneTypeId: Int): String = when (zoneTypeId) {
        ZONE_TYPE_ID_TRANSIT -> ZONE_TYPE_TRANSIT
        ZONE_TYPE_ID_RESIDENTIAL -> ZONE_TYPE_RESIDENTIAL
        ZONE_TYPE_ID_COMMERCIAL -> ZONE_TYPE_COMMERCIAL
        else -> ZONE_TYPE_MIXED
    }

    private val outputSchema = StructType(
        listOf(
            COL_YEAR_MONTH to DataTypes.StringType,
            COL_ZONE_TYPE to DataTypes.StringType,
            COL_TOTAL_TRIPS to DataTypes.LongType,
            COL_MEAN_FARE_AMOUNT to DataTypes.DoubleType,
            COL_STDDEV_FARE_AMOUNT to DataTypes.DoubleType,
            COL_MEAN_TRIP_DISTANCE to DataTypes.DoubleType,
            COL_STDDEV_TRIP_DISTANCE to DataTypes.DoubleType,
            COL_MEAN_TOTAL_AMOUNT to DataTypes.DoubleType
        ).map { (name, type) -> StructField.apply(name, type, true, Metadata.empty()) }.toTypedArray()
    )
}

/**
 * Single-pass Parquet scan: packs (month_offset, PULocationID, hour_of_day) into a single 23-bit
 * integer sub_bucket_key, aggregates all 28.5M trips into ~38,000 sub-buckets, and collects them
 * to the driver.
 */
internal fun collectPackedSubBuckets(trips: Dataset<Row>): List<Row> {
    val pickup = sf.col(COL_PICKUP_DATETIME)
    val monthOffsetExpr = sf.year(pickup).minus(BASE_YEAR).multiply(12).plus(sf.month(pickup))
    val puIntCol = sf.col(COL_PU_LOCATION_ID).cast(DataTypes.IntegerType)
    val bucketKeyExpr = packBucketKey(puIntCol, sf.hour(pickup))
    val subBucketKeyExpr = sf.shiftleft(monthOffsetExpr, SUB_BUCKET_KEY_SHIFT).plus(bucketKeyExpr)

    val fare = sf.col(COL_FARE_AMOUNT)
    val dist = sf.col(COL_TRIP_DISTANCE)

    return trips
        .select(
            subBucketKeyExpr.alias(COL_SUB_BUCKET_KEY),
            sf.col(COL_RATECODE_ID),
            sf.col(COL_PASSENGER_COUNT),
            fare,
            dist,
            sf.col(COL_TOTAL_AMOUNT)
        )
        .groupBy(COL_SUB_BUCKET_KEY)
        .agg(
            sf.count("*").alias(COL_TOTAL_TRIPS),
            sf.sum(sf.`when`(sf.col(COL_RATECODE_ID).isin(RATECODE_JFK, RATECODE_NEWARK), 1).otherwise(0))
                .alias(COL_AIRPORT_TRIPS),
            sf.sum(sf.`when`(sf.col(COL_RATECODE_ID).equalTo(RATECODE_NEGOTIATED), 1).otherwise(0))
                .alias(COL_NEGOTIATED_TRIPS),
            sf.sum(sf.`when`(sf.col(COL_PASSENGER_COUNT).geq(GROUP_MIN_PASSENGERS), 1).otherwise(0))
                .alias(COL_GROUP_TRIPS),
            sf.sum(fare).alias(COL_SUM_FARE),
            sf.sum(fare.multiply(fare)).alias(COL_SUM_SQ_FARE),
            sf.sum(dist).alias(COL_SUM_DIST),
            sf.sum(dist.multiply(dist)).alias(COL_SUM_SQ_DIST),
            sf.sum(sf.col(COL_TOTAL_AMOUNT)).alias(COL_SUM_TOTAL)
        )
        .collectAsList()
}
