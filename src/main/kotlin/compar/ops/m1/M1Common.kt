package compar.ops.m1

import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions as sf
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.NumericType

// Definitions from the assignment shared by M1V1, M1V2 and M1V3.
//
// Example of the final cleaned DataFrame (dfClean) returned by M1:
// +--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+----+
// |VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|trip_distance|RatecodeID|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|airport_fee|year|
// +--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+----+
// |1       |2015-01-01 00:11:33 |2015-01-01 00:16:48  |1              |1.0          |1         |N                 |41          |166         |1           |5.7        |0.5  |0.5    |1.4       |0.0         |0.0                  |8.4         |null                |null       |2015|
// |1       |2015-01-01 00:18:24 |2015-01-01 00:24:20  |1              |0.9          |1         |N                 |166         |238         |3           |6.0        |0.5  |0.5    |0.0       |0.0         |0.0                  |7.3         |null                |null       |2015|
// |1       |2015-01-01 00:26:19 |2015-01-01 00:41:06  |1              |3.5          |1         |N                 |238         |162         |1           |13.2       |0.5  |0.5    |2.9       |0.0         |0.0                  |17.4        |null                |null       |2015|
// +--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+----+

internal val invalidFare: Column = sf.col("fare_amount").leq(0).or(sf.col("fare_amount").isNull)
internal val invalidDistance: Column = sf.col("trip_distance").leq(0).or(sf.col("trip_distance").isNull)
internal val invalidPassenger: Column = sf.col("passenger_count").leq(0)
    .or(sf.col("passenger_count").gt(6))
    .or(sf.col("passenger_count").isNull)
internal val invalidTime: Column = sf.col("tpep_dropoff_datetime").leq(sf.col("tpep_pickup_datetime"))
    .or(sf.col("tpep_pickup_datetime").isNull)
    .or(sf.col("tpep_dropoff_datetime").isNull)
internal val invalidLocation: Column = sf.not(sf.col("PULocationID").between(1, 263))
    .or(sf.not(sf.col("DOLocationID").between(1, 263)))
    .or(sf.col("PULocationID").isNull)
    .or(sf.col("DOLocationID").isNull)

/** Valid records, written in positive form so Parquet can skip row groups. A null makes the condition null, which drops the row. */
internal val validRecord: Column = sf.col("fare_amount").gt(0)
    .and(sf.col("trip_distance").gt(0))
    .and(sf.col("passenger_count").between(1, 6))
    .and(sf.col("tpep_dropoff_datetime").gt(sf.col("tpep_pickup_datetime")))
    .and(sf.col("PULocationID").between(1, 263))
    .and(sf.col("DOLocationID").between(1, 263))

private val numericColumns = listOf(
    "passenger_count", "RatecodeID", "payment_type", "PULocationID",
    "DOLocationID", "trip_distance", "fare_amount", "total_amount"
)
private val timestampColumns = listOf("tpep_pickup_datetime", "tpep_dropoff_datetime")
private val validTimestampTypes = setOf(DataTypes.TimestampType, DataTypes.TimestampNTZType)

/**
 * Reads the Parquet files. A comma-separated list avoids the glob, which needs winutils on Windows.
 */
internal fun readTrips(spark: SparkSession, dataPath: String): Dataset<Row> =
    spark.read().parquet(*dataPath.split(",").toTypedArray())

/**
 * Checks the schema once and casts only the columns the operations can't use as they are.
 * This has no per-row cost for the columns that are already usable.
 */
internal fun castIfNeeded(input: Dataset<Row>, tag: String? = null): Dataset<Row> {
    val types = input.schema().fields().associate { it.name() to it.dataType() }
    val castedColumns = mutableListOf<String>()
    var result = input

    for (column in numericColumns) {
        if (types[column] !is NumericType) {
            result = result.withColumn(column, sf.col(column).cast(DataTypes.DoubleType))
            castedColumns.add(column)
        }
    }
    for (column in timestampColumns) {
        if (types[column] !in validTimestampTypes) {
            result = result.withColumn(column, sf.col(column).cast(DataTypes.TimestampType))
            castedColumns.add(column)
        }
    }

    println("[$tag] Casted columns: ${castedColumns.ifEmpty { listOf("none") }.joinToString(", ")}")
    return result
}

/** Adds [count] records with the given validity bitmask to the counters of their year. */
internal fun addToYear(counters: MutableMap<Int?, LongArray>, year: Int?, mask: Int, count: Long) {
    val row = counters.getOrPut(year) { LongArray(7) }
    row[0] += count
    for (bit in 0 until 5) {
        if (mask and (1 shl bit) != 0) row[bit + 1] += count
    }
    if (mask == 0) row[6] += count
}

/**
 * Prints the per-year table and totals. Each row holds: total, the 5 criteria, valid.
 * The table is formatted on the driver, because show() on a DataFrame costs about 1 s just to generate code.
 */
internal fun printCleaningStats(counters: Map<Int?, LongArray>, tag: String? = null) {
    println("\n--- [$tag] Yearly Cleaning Statistics (Removed Records) ---")
    val header = listOf(
        "year", "total_records", "invalid_fare", "invalid_distance", "invalid_passenger",
        "invalid_time", "invalid_location", "valid_records"
    )
    val rows = counters.entries.sortedBy { it.key ?: Int.MIN_VALUE }.map { (year, row) ->
        listOf(year?.toString() ?: "NULL") + row.map { it.toString() }
    }
    val widths = header.indices.map { column -> (rows.map { it[column] } + header[column]).maxOf { it.length } }
    val border = widths.joinToString("+", "+", "+") { "-".repeat(it) }
    fun line(cells: List<String>) = cells.indices.joinToString("|", "|", "|") { cells[it].padEnd(widths[it]) }
    println(border)
    println(line(header))
    println(border)
    rows.forEach { println(line(it)) }
    println(border)

    val totalRecords = counters.values.sumOf { it[0] }
    val validRecords = counters.values.sumOf { it[6] }
    val removedRecords = totalRecords - validRecords
    val pctRemoved = if (totalRecords > 0) (removedRecords.toDouble() / totalRecords.toDouble() * 100.0) else 0.0
    println(String.format("[$tag] Total records read: %,d", totalRecords))
    println(String.format("[$tag] Valid records: %,d (%.2f%%)", validRecords, 100.0 - pctRemoved))
    println(String.format("[$tag] Total removed records: %,d (%.2f%%)", removedRecords, pctRemoved))
}

