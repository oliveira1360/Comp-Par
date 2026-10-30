package compar.ops.m1

import compar.ops.DataLoader
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions as sf
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.NumericType
import org.apache.spark.storage.StorageLevel

/**
 * M1 v1 — TODO: optimization name.
 * Bottleneck (v0): TODO: what profiling showed.
 * Change: TODO: what changes compared to v0.
 */
class M1V1 : DataLoader {
    private val numericColumns = listOf(
        "passenger_count", "RatecodeID", "payment_type", "PULocationID",
        "DOLocationID", "trip_distance", "fare_amount", "total_amount"
    )
    private val timestampColumns = listOf("tpep_pickup_datetime", "tpep_dropoff_datetime")
    private val validTimestampTypes = setOf(DataTypes.TimestampType, DataTypes.TimestampNTZType)

    override fun load(spark: SparkSession, dataPath: String, sampleFraction: Double?): Dataset<Row> {
        println("\n[M1] A carregar dados de: $dataPath")
        val dfRaw = spark.read().parquet(dataPath)
        val teste = sampleFraction ?: 0.05 // sub the if condition

        var df = castIfNeeded(dfRaw)
            .withColumn("year", sf.year(sf.col("tpep_pickup_datetime")))



        df = df.sample(teste, 42L)

        df = df
            .select(
                "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
                "trip_distance", "RatecodeID", "PULocationID", "DOLocationID",
                "payment_type", "fare_amount", "total_amount", "year"
            )


        // Invalidity criteria (M1)
        val condInvalidFare = df.col("fare_amount").leq(0).or(sf.col("fare_amount").isNull)
        val condInvalidDistance = df.col("trip_distance").leq(0).or(sf.col("trip_distance").isNull)
        val condInvalidPassenger = df.col("passenger_count").leq(0)
            .or(df.col("passenger_count").gt(6))
            .or(df.col("passenger_count").isNull)
        val condInvalidTime = df.col("tpep_dropoff_datetime").leq(sf.col("tpep_pickup_datetime"))
            .or(df.col("tpep_pickup_datetime").isNull)
            .or(df.col("tpep_dropoff_datetime").isNull)
        val condInvalidLocation = sf.not(df.col("PULocationID").between(1, 263))
            .or(sf.not(df.col("DOLocationID").between(1, 263)))
            .or(df.col("PULocationID").isNull)
            .or(df.col("DOLocationID").isNull)


        val condValidAll = sf.not(condInvalidFare)
            .and(sf.not(condInvalidDistance))
            .and(sf.not(condInvalidPassenger))
            .and(sf.not(condInvalidTime))
            .and(sf.not(condInvalidLocation))


        fun countIf(cond: Column) = sf.sum(sf.`when`(cond, 1).otherwise(0))

        println("\n--- [M1] Estatísticas de Limpeza por Ano (Registos Removidos) ---")
        val cleaningStats = df
            .groupBy("year")
            .agg(
                sf.count("*").alias("total_records"),
                countIf(condInvalidFare).alias("invalid_fare"),
                countIf(condInvalidDistance).alias("invalid_distance"),
                countIf(condInvalidPassenger).alias("invalid_passenger"),
                countIf(condInvalidTime).alias("invalid_time"),
                countIf(condInvalidLocation).alias("invalid_location"),
                countIf(condValidAll).alias("valid_records")
            )
            .orderBy("year")
            .cache()

        val totals = cleaningStats.agg(sf.sum("total_records"), sf.sum("valid_records")).first()
        val totalRecords = totals.getLong(0)
        val validRecords = totals.getLong(1)

        val dfClean = df.filter(condValidAll)

        val removedRecords = totalRecords - validRecords
        val pctRemoved = if (totalRecords > 0) (removedRecords.toDouble() / totalRecords.toDouble() * 100.0) else 0.0
        println(String.format("[M1] Registos válidos: %,d (%.2f%%)", validRecords, 100.0 - pctRemoved))
        println(String.format("[M1] Registos removidos no total: %,d (%.2f%%)", removedRecords, pctRemoved))

        return dfClean
    }

    /**
     * Checks the schema once and casts only the columns the operations can't use as they are.
     * This has no per-row cost for the columns that are already usable.
     */
    private fun castIfNeeded(input: Dataset<Row>): Dataset<Row> {
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

        println("[M1] Colunas convertidas: ${castedColumns.ifEmpty { listOf("nenhuma") }.joinToString(", ")}")
        return result
    }
}
