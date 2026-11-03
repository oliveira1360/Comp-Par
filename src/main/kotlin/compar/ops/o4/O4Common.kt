package compar.ops.o4

import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions as sf

// ============================================================================
// Column Names (Constants to eliminate hardcoded strings)
// ============================================================================

// Input Trip Columns
internal const val COL_PU_LOCATION_ID = "PULocationID"
internal const val COL_PICKUP_DATETIME = "tpep_pickup_datetime"
internal const val COL_RATECODE_ID = "RatecodeID"
internal const val COL_PASSENGER_COUNT = "passenger_count"
internal const val COL_FARE_AMOUNT = "fare_amount"
internal const val COL_TRIP_DISTANCE = "trip_distance"
internal const val COL_TOTAL_AMOUNT = "total_amount"

// Temporal & Bucket Classification Columns
internal const val COL_HOUR_OF_DAY = "hour_of_day"
internal const val COL_YEAR_MONTH = "year_month"
internal const val COL_BUCKET_TOTAL_TRIPS = "bucket_total_trips"
internal const val COL_FRAC_AIRPORT = "frac_airport"
internal const val COL_FRAC_NEGOTIATED = "frac_negotiated"
internal const val COL_FRAC_GROUP = "frac_group"
internal const val COL_ZONE_TYPE = "zone_type"

// Output Aggregate Columns
internal const val COL_TOTAL_TRIPS = "total_trips"
internal const val COL_MEAN_FARE_AMOUNT = "mean_fare_amount"
internal const val COL_STDDEV_FARE_AMOUNT = "stddev_fare_amount"
internal const val COL_MEAN_TRIP_DISTANCE = "mean_trip_distance"
internal const val COL_STDDEV_TRIP_DISTANCE = "stddev_trip_distance"
internal const val COL_MEAN_TOTAL_AMOUNT = "mean_total_amount"

// ============================================================================
// Domain Constants & Default Thresholds
// ============================================================================

internal const val RATECODE_JFK = 2
internal const val RATECODE_NEWARK = 3
internal const val RATECODE_NEGOTIATED = 5
internal const val GROUP_MIN_PASSENGERS = 4

internal const val DEFAULT_AIRPORT_THRESHOLD = 0.10
internal const val DEFAULT_NEGOTIATED_THRESHOLD = 0.01
internal const val DEFAULT_GROUP_THRESHOLD = 0.08
internal const val DEFAULT_COMMERCIAL_MAX_GROUP = 0.04

internal const val ZONE_TYPE_TRANSIT = "transit"
internal const val ZONE_TYPE_RESIDENTIAL = "residential"
internal const val ZONE_TYPE_COMMERCIAL = "commercial"
internal const val ZONE_TYPE_MIXED = "mixed"

// ============================================================================
// Shared Projections, Classification Logic & Storage
// ============================================================================

/** Minimal set of 7 columns needed for O4 from input.trips. */
internal val o4TripColumns = arrayOf(
    sf.col(COL_PU_LOCATION_ID),
    sf.col(COL_PICKUP_DATETIME),
    sf.col(COL_RATECODE_ID),
    sf.col(COL_PASSENGER_COUNT),
    sf.col(COL_FARE_AMOUNT),
    sf.col(COL_TRIP_DISTANCE),
    sf.col(COL_TOTAL_AMOUNT)
)

/**
 * Classifies a (PULocationID, hour_of_day) bucket into one of the 4 zone types
 * based on the three trip-type fractions and configurable thresholds:
 * 1. transit:     frac_airport >= airportThreshold
 * 2. residential: frac_group >= groupThreshold
 * 3. commercial:  frac_negotiated >= negotiatedThreshold OR frac_group <= commercialMaxGroup
 * 4. mixed:       otherwise
 */
internal fun classifyZoneTypeExpr(
    fracAirportCol: Column = sf.col(COL_FRAC_AIRPORT),
    fracNegotiatedCol: Column = sf.col(COL_FRAC_NEGOTIATED),
    fracGroupCol: Column = sf.col(COL_FRAC_GROUP),
    airportThreshold: Double = DEFAULT_AIRPORT_THRESHOLD,
    negotiatedThreshold: Double = DEFAULT_NEGOTIATED_THRESHOLD,
    groupThreshold: Double = DEFAULT_GROUP_THRESHOLD,
    commercialMaxGroup: Double = DEFAULT_COMMERCIAL_MAX_GROUP
): Column = sf.`when`(
    fracAirportCol.geq(airportThreshold),
    sf.lit(ZONE_TYPE_TRANSIT)
).`when`(
    fracGroupCol.geq(groupThreshold),
    sf.lit(ZONE_TYPE_RESIDENTIAL)
).`when`(
    fracNegotiatedCol.geq(negotiatedThreshold).or(fracGroupCol.leq(commercialMaxGroup)),
    sf.lit(ZONE_TYPE_COMMERCIAL)
).otherwise(
    sf.lit(ZONE_TYPE_MIXED)
)

/** Writes the resulting monthly zone-type statistics DataFrame in Parquet format. */
internal fun writeTripTypeStats(df: Dataset<Row>, outputDir: String) {
    df.write()
        .mode(SaveMode.Overwrite)
        .parquet("$outputDir/o4_trip_types")
}
