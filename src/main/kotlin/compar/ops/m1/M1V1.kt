package compar.ops.m1

import compar.ops.DataLoader
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions as sf

/**
 * M1 v1 — one scan and casts only where needed.
 * Bottleneck (v0): two count() calls and the statistics each scan all the data, which makes 3 full
 * scans, and the casts run on every row even when the type is already usable.
 * Change: one aggregation computes the total, the 5 criteria and the valid records in the same scan,
 * and the schema is checked once so that only the columns that need it are cast.
 */
class M1V1 : DataLoader {

    override fun load(spark: SparkSession, dataPath: String, sampleFraction: Double?): Dataset<Row> {
        println("\n[$tag] Loading data from: $dataPath")
        var df = castIfNeeded(readTrips(spark, dataPath), tag)
            .withColumn("year", sf.year(sf.col("tpep_pickup_datetime")))

        if (sampleFraction != null && sampleFraction > 0.0 && sampleFraction < 1.0) {
            println(String.format("[$tag] Applying sampling of %.1f%%", sampleFraction * 100.0))
            df = df.sample(sampleFraction, 42L)
        }

        val rows = df.groupBy("year")
            .agg(
                sf.count("*"),
                countIf(invalidFare),
                countIf(invalidDistance),
                countIf(invalidPassenger),
                countIf(invalidTime),
                countIf(invalidLocation),
                countIf(validRecord)
            )
            .collectAsList()

        val counters = rows.associate { row ->
            val year = if (row.isNullAt(0)) null else row.getInt(0)
            year to LongArray(7) { row.getLong(it + 1) }
        }
        printCleaningStats(counters, tag)

        return df.filter(validRecord)
    }

    private fun countIf(condition: Column) = sf.sum(sf.`when`(condition, 1).otherwise(0))
}
