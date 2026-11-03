package compar.ops.o4

import compar.ops.Analysis
import compar.ops.AnalysisInput
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions as sf

/**
 * O4 — Trip-Type Classification (Baseline):
 * 1. Derives temporal columns (hour_of_day, year_month) on the full trip table.
 * 2. Computes the airport (RatecodeID in {2, 3}), negotiated (RatecodeID = 5), and
 *    group (passenger_count >= 4) trip fractions per (PULocationID, hour_of_day) bucket.
 * 3. Classifies each bucket as 'residential', 'commercial', 'transit', or 'mixed'
 *    based on these fractions and configurable thresholds.
 * 4. Joins the classification back to the full trip table.
 * 5. Computes fare and distance statistics per zone type per month and writes
 *    the result in Parquet format to output/o4_trip_types.
 */
class O4V0Baseline(
    private val airportThreshold: Double = DEFAULT_AIRPORT_THRESHOLD,
    private val negotiatedThreshold: Double = DEFAULT_NEGOTIATED_THRESHOLD,
    private val groupThreshold: Double = DEFAULT_GROUP_THRESHOLD,
    private val commercialMaxGroup: Double = DEFAULT_COMMERCIAL_MAX_GROUP
) : Analysis {

    override fun run(input: AnalysisInput): Dataset<Row> {
        println("\n--- [$tag] Trip-Type Classification ---")

        // 1. Derive temporal columns (baseline with chained withColumn calls on all columns)
        val dfPrepared = input.trips
            .withColumn(COL_HOUR_OF_DAY, sf.hour(sf.col(COL_PICKUP_DATETIME)))
            .withColumn(COL_YEAR_MONTH, sf.date_format(sf.col(COL_PICKUP_DATETIME), "yyyy-MM"))

        // 2. Compute classified (PULocationID, hour_of_day) buckets
        val classifiedBuckets = classifyBucketsBaseline(
            dfPrepared = dfPrepared,
            airportThreshold = airportThreshold,
            negotiatedThreshold = negotiatedThreshold,
            groupThreshold = groupThreshold,
            commercialMaxGroup = commercialMaxGroup
        )

        // 3. Join the classification back to the full trip table
        val tripsWithZoneType = dfPrepared
            .join(classifiedBuckets, arrayOf(COL_PU_LOCATION_ID, COL_HOUR_OF_DAY))

        // 4. Compute fare and distance statistics per zone type and month
        val o4Stats = aggregateMonthlyZoneTypeStats(tripsWithZoneType)

        writeTripTypeStats(o4Stats, input.outputDir)

        return o4Stats
    }
}

/**
 * Aggregates trips per (PULocationID, hour_of_day) bucket to compute the 3 trip-type fractions
 * and classifies each bucket into 'transit', 'residential', 'commercial', or 'mixed'.
 */
internal fun classifyBucketsBaseline(
    dfPrepared: Dataset<Row>,
    airportThreshold: Double = DEFAULT_AIRPORT_THRESHOLD,
    negotiatedThreshold: Double = DEFAULT_NEGOTIATED_THRESHOLD,
    groupThreshold: Double = DEFAULT_GROUP_THRESHOLD,
    commercialMaxGroup: Double = DEFAULT_COMMERCIAL_MAX_GROUP
): Dataset<Row> {
    val bucketFractions = dfPrepared
        .groupBy(COL_PU_LOCATION_ID, COL_HOUR_OF_DAY)
        .agg(
            sf.count("*").alias(COL_BUCKET_TOTAL_TRIPS),
            sf.avg(sf.`when`(sf.col(COL_RATECODE_ID).isin(RATECODE_JFK, RATECODE_NEWARK), 1.0).otherwise(0.0))
                .alias(COL_FRAC_AIRPORT),
            sf.avg(sf.`when`(sf.col(COL_RATECODE_ID).equalTo(RATECODE_NEGOTIATED), 1.0).otherwise(0.0))
                .alias(COL_FRAC_NEGOTIATED),
            sf.avg(sf.`when`(sf.col(COL_PASSENGER_COUNT).geq(GROUP_MIN_PASSENGERS), 1.0).otherwise(0.0))
                .alias(COL_FRAC_GROUP)
        )

    val zoneTypeExpr = classifyZoneTypeExpr(
        airportThreshold = airportThreshold,
        negotiatedThreshold = negotiatedThreshold,
        groupThreshold = groupThreshold,
        commercialMaxGroup = commercialMaxGroup
    )

    return bucketFractions
        .withColumn(COL_ZONE_TYPE, zoneTypeExpr)
        .select(COL_PU_LOCATION_ID, COL_HOUR_OF_DAY, COL_ZONE_TYPE)
}

/**
 * Computes monthly fare and distance statistics broken down by (year_month, zone_type).
 */
internal fun aggregateMonthlyZoneTypeStats(tripsWithZoneType: Dataset<Row>): Dataset<Row> = tripsWithZoneType
    .groupBy(COL_YEAR_MONTH, COL_ZONE_TYPE)
    .agg(
        sf.count("*").alias(COL_TOTAL_TRIPS),
        sf.avg(COL_FARE_AMOUNT).alias(COL_MEAN_FARE_AMOUNT),
        sf.stddev(COL_FARE_AMOUNT).alias(COL_STDDEV_FARE_AMOUNT),
        sf.avg(COL_TRIP_DISTANCE).alias(COL_MEAN_TRIP_DISTANCE),
        sf.stddev(COL_TRIP_DISTANCE).alias(COL_STDDEV_TRIP_DISTANCE),
        sf.avg(COL_TOTAL_AMOUNT).alias(COL_MEAN_TOTAL_AMOUNT)
    )
    .orderBy(COL_YEAR_MONTH, COL_ZONE_TYPE)
