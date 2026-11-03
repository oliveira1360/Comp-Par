package compar.ops.o1

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
internal const val COL_FARE_AMOUNT = "fare_amount"
internal const val COL_TRIP_DISTANCE = "trip_distance"
internal const val COL_PAYMENT_TYPE = "payment_type"

// Zone Lookup Table Columns
internal const val COL_PICKUP_BOROUGH = "pickup_borough"

// Temporal & Derived Metric Columns
internal const val COL_HOUR_OF_DAY = "hour_of_day"
internal const val COL_DAY_OF_WEEK = "day_of_week"
internal const val COL_DAY_TYPE = "day_type"
internal const val COL_FARE_PER_KM = "fare_per_km"
internal const val COL_MEAN_FARE_PER_KM = "mean_fare_per_km"
internal const val COL_STDDEV_FARE_PER_KM = "stddev_fare_per_km"
internal const val COL_IS_ANOMALY = "is_anomaly"

// Output Aggregate Columns
internal const val COL_TOTAL_TRIPS = "total_trips"
internal const val COL_ANOMALY_TRIPS = "anomaly_trips"
internal const val COL_ANOMALY_RATE_PCT = "anomaly_rate_pct"

// ============================================================================
// Domain Constants
// ============================================================================

internal const val KM_PER_MILE = 1.60934
internal const val ANOMALY_SIGMA_FACTOR = 3.0
internal const val DAY_TYPE_WEEKEND = "Weekend"
internal const val DAY_TYPE_WEEKDAY = "Weekday"

// ============================================================================
// Shared Projections, Formulas & Storage
// ============================================================================

/** Minimal set of columns needed for O1 from input.trips. */
internal val o1TripColumns = arrayOf(
    sf.col(COL_PU_LOCATION_ID),
    sf.col(COL_PICKUP_DATETIME),
    sf.col(COL_FARE_AMOUNT),
    sf.col(COL_TRIP_DISTANCE),
    sf.col(COL_PAYMENT_TYPE)
)

/** Formula: fare per kilometer = fare_amount / (trip_distance * 1.60934). */
internal fun farePerKm(): Column =
    sf.col(COL_FARE_AMOUNT).divide(sf.col(COL_TRIP_DISTANCE).multiply(KM_PER_MILE))

/** Formula: Day type is "Weekend" on Sunday (1) and Saturday (7), else "Weekday". */
internal fun dayTypeExpr(dayOfWeekCol: Column = sf.col(COL_DAY_OF_WEEK)): Column =
    sf.`when`(dayOfWeekCol.isin(1, 7), sf.lit(DAY_TYPE_WEEKEND))
        .otherwise(sf.lit(DAY_TYPE_WEEKDAY))

/**
 * Evaluates whether a trip is a statistical fare anomaly based on the 3-sigma rule:
 *
 * Mathematical condition:
 *   threshold = μ + 3 * σ
 *   is_anomaly = (fare_per_km > threshold) ∧ (σ > 0)
 *
 * Why each component is required:
 * 1. threshold (μ + 3σ): The statistical upper cutoff. In a normal distribution, values
 *    exceeding μ + 3σ fall in the top 0.15% tail (outliers).
 * 2. stddev > 0.0: Protects against zero variance or single-trip buckets. If a bucket has only
 *    one trip during the period (sample stddev = NULL) or identical flat-rate fares (stddev = 0),
 *    normal trips must NOT be flagged as anomalies.
 * 3. coalesce(..., false): Null-safety protection. If stddev is NULL or NaN (from single-element
 *    windows or corrupted metrics), SQL comparisons yield NULL. coalesce guarantees a strict,
 *    non-null Boolean output (true or false).
 */
internal fun anomalyCondition(
    fareCol: Column,
    meanCol: Column,
    stddevCol: Column,
    sigmaFactor: Double = ANOMALY_SIGMA_FACTOR
): Column {
    val threshold = meanCol.plus(stddevCol.multiply(sigmaFactor))
    return sf.coalesce(
        stddevCol.gt(0.0).and(fareCol.gt(threshold)),
        sf.lit(false)
    )
}

/** Writes the resulting anomaly rates DataFrame in Parquet format. */
internal fun writeAnomalyRates(df: Dataset<Row>, outputDir: String) {
    df.write()
        .mode(SaveMode.Overwrite)
        .parquet(outputDir)
}
