package compar.ops.m2

import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions as sf
import org.apache.spark.sql.types.DataTypes

// ============================================================================
// Column Names (Constants to eliminate hardcoded strings)
// ============================================================================

// Input Trip Columns
internal const val COL_PU_LOCATION_ID = "PULocationID"
internal const val COL_DO_LOCATION_ID = "DOLocationID"
internal const val COL_PICKUP_DATETIME = "tpep_pickup_datetime"
internal const val COL_DROPOFF_DATETIME = "tpep_dropoff_datetime"
internal const val COL_FARE_AMOUNT = "fare_amount"
internal const val COL_TRIP_DISTANCE = "trip_distance"

// Zone Lookup Table Columns
internal const val COL_LOCATION_ID = "LocationID"
internal const val COL_BOROUGH = "Borough"
internal const val ZONE_LOCATION_ID_IDX = 0
internal const val ZONE_BOROUGH_IDX = 1
internal const val UNKNOWN_BOROUGH = "Unknown"

// Temporal Columns (Derived)
internal const val COL_PICKUP_BOROUGH = "pickup_borough"
internal const val COL_DAY_OF_WEEK = "day_of_week"
internal const val COL_HOUR_OF_DAY = "hour_of_day"
internal const val COL_TRIP_DURATION_MIN = "trip_duration_min"

// Aggregate Output Columns
internal const val COL_TOTAL_TRIPS = "total_trips"
internal const val COL_MEAN_FARE_AMOUNT = "mean_fare_amount"
internal const val COL_MEAN_TRIP_DURATION_MIN = "mean_trip_duration_min"
internal const val COL_MEAN_TRIP_DISTANCE = "mean_trip_distance"

// Intermediate Packing Keys
internal const val COL_BOROUGH_DAY_HOUR_KEY = "borough_day_hour_key"
internal const val COL_ZONE_DAY_HOUR_KEY = "zone_day_hour_key"
internal const val COL_ZONE_TRIPS = "zone_trips"
internal const val COL_SUM_FARE = "sum_fare"
internal const val COL_SUM_DURATION = "sum_duration"
internal const val COL_SUM_DISTANCE = "sum_distance"

// ============================================================================
// Bitpacking Constants
// ============================================================================

internal const val MAX_LOCATION_ID = 263
internal const val HOUR_BITS = 5                               // 0..23 fits in 5 bits (max 31)
internal const val DAY_BITS = 3                                // 1..7 fits in 3 bits (max 7)
internal const val DAY_SHIFT = HOUR_BITS                       // shift day_of_week by 5 bits
internal const val ZONE_SHIFT = HOUR_BITS + DAY_BITS           // shift PULocationID by 8 bits
internal const val BOROUGH_SHIFT = HOUR_BITS + DAY_BITS        // shift boroughId by 8 bits
internal const val HOUR_MASK = (1 shl HOUR_BITS) - 1           // 0b11111 = 31
internal const val DAY_MASK = (1 shl DAY_BITS) - 1             // 0b111 = 7

// ============================================================================
// Shared Expressions & Write Operations
// ============================================================================

internal val m2TripColumns = arrayOf(
    sf.col(COL_PU_LOCATION_ID),
    sf.col(COL_PICKUP_DATETIME),
    sf.col(COL_DROPOFF_DATETIME),
    sf.col(COL_FARE_AMOUNT),
    sf.col(COL_TRIP_DISTANCE)
)

/** Direct timestamp difference in seconds (avoids unix_timestamp timezone overhead). */
internal fun tripDurationSeconds(): Column =
    sf.col(COL_DROPOFF_DATETIME).cast(DataTypes.TimestampType).cast(DataTypes.LongType)
        .minus(sf.col(COL_PICKUP_DATETIME).cast(DataTypes.TimestampType).cast(DataTypes.LongType))

/** Writes the resulting DataFrame partitioned by pickup_borough in Parquet format. */
internal fun writeTemporalDemand(df: Dataset<Row>, outputDir: String) {
    df.write()
        .mode(SaveMode.Overwrite)
        .partitionBy(COL_PICKUP_BOROUGH)
        .parquet(outputDir)
}
