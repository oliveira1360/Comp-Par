package compar.ops.m1

import compar.ops.DataLoader
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions as sf

/**
 * M1 v2 — validity bitmask.
 * Bottleneck (v1): each of the 7 sums evaluates its own condition for every row.
 * Change: each row gets one integer with one bit per criterion, and the aggregation only counts
 * rows per (year, mask). That gives at most a few dozen groups, and the driver derives all the
 * counters from them, so the table needs no second job.
 */
class M1V2 : DataLoader {

    override fun load(spark: SparkSession, dataPath: String, sampleFraction: Double?): Dataset<Row> {
        val tag = javaClass.simpleName
        println("\n[$tag] Loading data from: $dataPath")
        var df = castIfNeeded(readTrips(spark, dataPath), tag)
            .withColumn("year", sf.year(sf.col("tpep_pickup_datetime")))

        if (sampleFraction != null && sampleFraction > 0.0 && sampleFraction < 1.0) {
            println(String.format("[$tag] Applying sampling of %.1f%%", sampleFraction * 100.0))
            df = df.sample(sampleFraction, 42L)
        }

        val mask = bit(invalidFare, 1)
            .plus(bit(invalidDistance, 2))
            .plus(bit(invalidPassenger, 4))
            .plus(bit(invalidTime, 8))
            .plus(bit(invalidLocation, 16))

        val counters = HashMap<Int?, LongArray>()
        for (row in df.groupBy(sf.col("year"), mask.alias("mask")).count().collectAsList()) {
            val year = if (row.isNullAt(0)) null else row.getInt(0)
            addToYear(counters, year, row.getInt(1), row.getLong(2))
        }
        printCleaningStats(counters, tag)

        return df.filter(validRecord)
    }

    private fun bit(condition: org.apache.spark.sql.Column, value: Int) =
        sf.`when`(condition, value).otherwise(0)
}
