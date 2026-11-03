package compar.ops.m1

import compar.ops.DataLoader
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.parquet.VectorizedParquetRecordReader
import org.apache.spark.sql.functions as sf
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import java.io.Serializable
import java.time.LocalDate

/**
 * M1 v3 — direct columnar scan.
 * Bottleneck (v2): Spark turns each column batch into rows and runs a shuffle, yet decoding Parquet
 * is the only cost that cannot be avoided.
 * Change: tasks read the column batches directly, build the bitmasks in tight loops and count per
 * (day, mask) in an array, with no rows and no shuffle. The year is computed once per day on the driver.
 * Runs that need Spark's row handling (sampling, or other column types) use M1V2 instead.
 */
class M1V3 : DataLoader {

    private val expectedTypes: Map<String, DataType> = mapOf(
        "tpep_pickup_datetime" to DataTypes.TimestampNTZType,
        "tpep_dropoff_datetime" to DataTypes.TimestampNTZType,
        "passenger_count" to DataTypes.LongType,
        "trip_distance" to DataTypes.DoubleType,
        "PULocationID" to DataTypes.LongType,
        "DOLocationID" to DataTypes.LongType,
        "fare_amount" to DataTypes.DoubleType
    )

    override fun load(spark: SparkSession, dataPath: String, sampleFraction: Double?): Dataset<Row> {
        val raw = readTrips(spark, dataPath)
        val types = raw.schema().fields().associate { it.name() to it.dataType() }
        val isSampled = sampleFraction != null && sampleFraction > 0.0 && sampleFraction < 1.0
        if (isSampled || expectedTypes.any { (column, type) -> types[column] != type }) {
            return M1V2().load(spark, dataPath, sampleFraction)
        }

        println("\n[$tag] Loading data from: $dataPath")
        println("[$tag] Casted columns: none")
        val files = raw.inputFiles().toList()
        val jsc = JavaSparkContext.fromSparkContext(spark.sparkContext())
        val perFile = jsc.parallelize(files, files.size).mapPartitions(ScanFiles(expectedTypes.keys.toList())).collect()

        val perDay = HashMap<Int, LongArray>()
        for (result in perFile) {
            for ((day, counts) in result) {
                val total = perDay.getOrPut(day) { LongArray(MASKS) }
                for (mask in 0 until MASKS) total[mask] += counts[mask]
            }
        }

        val counters = HashMap<Int?, LongArray>()
        for ((day, counts) in perDay) {
            val year = if (day == NULL_DAY) null else LocalDate.ofEpochDay(day.toLong()).year
            for (mask in 0 until MASKS) {
                if (counts[mask] != 0L) addToYear(counters, year, mask, counts[mask])
            }
        }
        printCleaningStats(counters, tag)

        return raw.withColumn("year", sf.year(sf.col("tpep_pickup_datetime"))).filter(validRecord)
    }
}

private const val MASKS = 32
private const val BATCH_SIZE = 32768
private const val NULL_DAY = Int.MIN_VALUE
private const val MICROS_PER_DAY = 86_400_000_000L

/**
 * Runs on the executors: counts the records of its files per (day, mask).
 * The column order in [columns] must match the order of the column(n) calls below.
 */
private class ScanFiles(private val columns: List<String>) :
    FlatMapFunction<Iterator<String>, HashMap<Int, LongArray>>, Serializable {

    override fun call(files: Iterator<String>): Iterator<HashMap<Int, LongArray>> {
        val perDay = HashMap<Int, LongArray>()
        val masks = IntArray(BATCH_SIZE)
        var currentDay = Int.MAX_VALUE
        var currentCounts = LongArray(0)

        while (files.hasNext()) {
            val reader = VectorizedParquetRecordReader(false, BATCH_SIZE)
            reader.initialize(files.next(), columns)
            val batch = reader.resultBatch()
            val pickup = batch.column(0)
            val dropOff = batch.column(1)
            val passengers = batch.column(2)
            val distance = batch.column(3)
            val pickupZone = batch.column(4)
            val dropOffZone = batch.column(5)
            val fare = batch.column(6)

            while (reader.nextBatch()) {
                val rows = batch.numRows()
                for (i in 0 until rows) masks[i] = if (fare.isNullAt(i) || fare.getDouble(i) <= 0.0) 1 else 0
                for (i in 0 until rows) if (distance.isNullAt(i) || distance.getDouble(i) <= 0.0) masks[i] = masks[i] or 2
                for (i in 0 until rows) {
                    if (passengers.isNullAt(i)) {
                        masks[i] = masks[i] or 4
                    } else {
                        val count = passengers.getLong(i)
                        if (count !in 1..6) masks[i] = masks[i] or 4
                    }
                }
                for (i in 0 until rows) {
                    if (pickup.isNullAt(i) || dropOff.isNullAt(i) || dropOff.getLong(i) <= pickup.getLong(i)) {
                        masks[i] = masks[i] or 8
                    }
                }
                for (i in 0 until rows) {
                    if (pickupZone.isNullAt(i) || dropOffZone.isNullAt(i)) {
                        masks[i] = masks[i] or 16
                    } else {
                        val from = pickupZone.getLong(i)
                        val to = dropOffZone.getLong(i)
                        if (from !in 1..263 || to < 1 || to > 263) masks[i] = masks[i] or 16
                    }
                }
                for (i in 0 until rows) {
                    val day = if (pickup.isNullAt(i)) NULL_DAY else Math.floorDiv(pickup.getLong(i), MICROS_PER_DAY).toInt()
                    if (day != currentDay) {
                        currentDay = day
                        currentCounts = perDay.getOrPut(day) { LongArray(MASKS) }
                    }
                    currentCounts[masks[i]]++
                }
            }
            reader.close()
        }
        return listOf(perDay).iterator()
    }
}
