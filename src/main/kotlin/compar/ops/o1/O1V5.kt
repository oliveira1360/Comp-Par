package compar.ops.o1

import compar.ops.Analysis
import compar.ops.AnalysisInput
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions as sf
import org.apache.spark.sql.types.DataTypes
import java.io.File
import java.net.URI

// ============================================================================
// O1 v5 Constants
// ============================================================================

internal const val MAX_LOCATION_ID = 263
internal const val MAX_HOUR_OF_DAY = 23
internal const val MAX_BUCKET_KEY = (MAX_LOCATION_ID shl BUCKET_ZONE_SHIFT) + MAX_HOUR_OF_DAY // 8439
internal const val TOTAL_BUCKET_ARRAY_SIZE = MAX_BUCKET_KEY + 1 // 8440

/**
 * O1 v5 — Complete join elimination via O(1) array lookup and coalesce(1) write.
 *
 * Bottleneck (v4):
 * - Even with BroadcastHashJoin, Spark builds an in-memory hash table (HashedRelation) and performs
 *   a 64-bit hash computation and bucket probe for every single trip across millions of records.
 * - Collecting the zone lookup table through Spark launches cluster stages for a tiny 12 KB file.
 * - Writing the final <= 84 aggregated rows without coalesce leaves files fragmented across partitions.
 *
 * Change:
 * - Direct driver parsing of taxi_zone_lookup.csv to build an O(1) borough lookup array (zero Spark stages).
 * - Collects the <= 6,312 bucket thresholds to the driver and builds a flat 67 KB DoubleArray indexed
 *   directly by bucket_key (1..8440), replacing the BroadcastHashJoin with O(1) element_at lookup.
 * - Entire trips pipeline runs with zero joins (both zone lookup and threshold lookup are O(1) array accesses).
 * - Coalesces the <= 84 output rows into 1 partition for clean, single-file Parquet output.
 * - Reuses selectTripsWithBucketKey(), packRateKey(), aggregateByRateKey(), and decodeRateKey() from v4.
 */
class O1V5 : Analysis {

    override fun run(input: AnalysisInput): Dataset<Row> {
        println("\n--- [$tag] Fare Anomaly Detection ---")

        // Step 1: Parse the 12 KB zone lookup CSV directly on the driver to avoid Spark stage overhead
        val (boroughs, boroughLookupArray) = buildDriverBoroughArray(input.zones)

        // Step 2: Pre-aggregate bucket statistics and construct the flat O(1) threshold array on the driver
        val thresholdLookupArray = buildDriverThresholdArray(input.trips)

        // Step 3: Project minimal columns and pack the 14-bit bucket_key (reused from v4)
        val tripsWithBucketKey = selectTripsWithBucketKey(input.trips)

        // Step 4: O(1) threshold lookup via element_at (eliminates BroadcastHashJoin!)
        val isAnomalyExpr = isAnomalyFromArray(
            bucketKeyCol = sf.col(COL_BUCKET_KEY),
            fareMetricCol = sf.col(COL_FARE_PER_KM),
            thresholdLookupArray = thresholdLookupArray
        )
        val tripsFlagged = tripsWithBucketKey.withColumn(COL_IS_ANOMALY, isAnomalyExpr)

        // Step 5: O(1) borough lookup via element_at (LocationID is 1-based: 1..263)
        val boroughIdCol = lookupBoroughId(boroughLookupArray)

        // Step 6: Pack 8-bit rate_key and aggregate millions of trips (reused from v4)
        val rateKey = packRateKey(boroughIdCol, sf.col(COL_PAYMENT_TYPE), sf.col(COL_PICKUP_DATETIME))
        val aggregated = aggregateByRateKey(tripsFlagged, rateKey)

        // Step 7: Decode Strings on <= 84 output rows (reused from v4) and coalesce(1)
        val anomalyRatesDf = decodeRateKey(aggregated, boroughs)
            .orderBy(COL_PICKUP_BOROUGH, COL_PAYMENT_TYPE, COL_DAY_TYPE)
            .coalesce(1)

        writeAnomalyRates(anomalyRatesDf, input.outputDir)

        return anomalyRatesDf
    }
}

// ============================================================================
// Driver Array Construction & Lookup Functions (Introduced in v5)
// ============================================================================

/**
 * Evaluates whether a trip is anomalous in O(1) time by indexing directly into the driver-constructed
 * threshold array using the 14-bit bucket_key (1-based for Spark element_at).
 */
internal fun isAnomalyFromArray(
    bucketKeyCol: Column,
    fareMetricCol: Column,
    thresholdLookupArray: Column
): Column {
    val bucketKeyOneBased = bucketKeyCol.plus(1).cast(DataTypes.IntegerType)
    val thresholdCol = sf.element_at(thresholdLookupArray, bucketKeyOneBased)
    return sf.coalesce(
        thresholdCol.gt(0.0).and(fareMetricCol.gt(thresholdCol)),
        sf.lit(false)
    )
}

/** Resolves PULocationID (1..263) to its 3-bit borough index (0..6) in O(1) via element_at. */
internal fun lookupBoroughId(boroughLookupArray: Column): Column =
    sf.element_at(boroughLookupArray, sf.col(COL_PU_LOCATION_ID).cast(DataTypes.IntegerType))

/**
 * Pre-aggregates bucket statistics and builds an O(1) DoubleArray of thresholds (<= 8,440 elements, ~67 KB).
 */
internal fun buildDriverThresholdArray(
    trips: Dataset<Row>,
    metricExpr: Column = farePerKm()
): Column {
    val bucketKeyCol = packBucketKey(sf.col(COL_PU_LOCATION_ID), sf.hour(sf.col(COL_PICKUP_DATETIME)))
    val bucketStatsRows = trips
        .select(bucketKeyCol.alias(COL_BUCKET_KEY), metricExpr.alias(COL_FARE_PER_KM))
        .groupBy(COL_BUCKET_KEY)
        .agg(
            sf.avg(COL_FARE_PER_KM).alias(COL_MEAN_FARE_PER_KM),
            sf.stddev(COL_FARE_PER_KM).alias(COL_STDDEV_FARE_PER_KM)
        )
        .collectAsList()

    return createThresholdSparkArray(bucketStatsRows)
}

/**
 * Converts collected (bucket_key, mean, stddev) rows into a flat O(1) Spark array of (μ + 3σ) thresholds.
 */
internal fun createThresholdSparkArray(bucketStatsRows: List<Row>): Column {
    val thresholdTable = DoubleArray(TOTAL_BUCKET_ARRAY_SIZE) { 0.0 }
    for (row in bucketStatsRows) {
        if (!row.isNullAt(0) && !row.isNullAt(1) && !row.isNullAt(2)) {
            val key = row.getAs<Number>(0).toInt()
            val mean = row.getAs<Number>(1).toDouble()
            val stddev = row.getAs<Number>(2).toDouble()
            if (key in 0..MAX_BUCKET_KEY && stddev > 0.0) {
                thresholdTable[key] = mean + ANOMALY_SIGMA_FACTOR * stddev
            }
        }
    }

    return sf.array(*thresholdTable.map { sf.lit(it) }.toTypedArray())
}

/**
 * Parses taxi_zone_lookup.csv directly on the driver to construct an O(1) IntArray mapping LocationID -> borough_id.
 */
internal fun buildDriverBoroughArray(zones: Dataset<Row>): Pair<List<String>, Column> {
    val localFilePath = zones.inputFiles().firstOrNull()?.let { uriStr ->
        try {
            val uri = URI(uriStr)
            if (uri.scheme == null || uri.scheme == "file") uri.path else null
        } catch (_: Exception) {
            null
        }
    }

    val rows: List<Pair<Int, String>> = if (localFilePath != null && File(localFilePath).exists()) {
        File(localFilePath).bufferedReader().useLines { lines ->
            lines.drop(1).mapNotNull { line ->
                val tokens = line.split(",")
                if (tokens.size >= 2) {
                    val id = tokens[0].trim().toIntOrNull()
                    val boro = tokens[1].trim().trim('"')
                    if (id != null && boro.isNotEmpty()) id to boro else null
                } else null
            }.toList()
        }
    } else {
        // Fallback to Spark collection if local file path is not directly accessible
        val zoneRows = zones.select("LocationID", "Borough").collectAsList()
        zoneRows.mapNotNull {
            if (!it.isNullAt(0) && !it.isNullAt(1)) {
                it.getAs<Number>(0).toInt() to it.getString(1)
            } else null
        }
    }

    val boroughs = rows.map { it.second }.distinct().sorted()
    val boroughIndex = boroughs.withIndex().associate { (idx, name) -> name to idx }
    val unknownIndex = boroughs.size

    // Spark element_at is 1-based, so array index 0 corresponds to LocationID = 1
    val lookupTable = IntArray(MAX_LOCATION_ID) { unknownIndex }
    for ((locationId, boroName) in rows) {
        if (locationId in 1..MAX_LOCATION_ID) {
            lookupTable[locationId - 1] = boroughIndex[boroName] ?: unknownIndex
        }
    }

    val sparkArray = sf.array(*lookupTable.map { sf.lit(it) }.toTypedArray())
    return Pair(boroughs, sparkArray)
}
