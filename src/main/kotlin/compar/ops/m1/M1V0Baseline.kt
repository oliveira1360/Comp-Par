package compar.ops.m1

import compar.ops.DataLoader
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions as sf
import org.apache.spark.sql.types.DataTypes

/**
 * M1 — Data Ingestion and Cleaning:
 * 1. Loads the trip Parquet files with explicit type casts.
 * 2. Applies sampling if sampleFraction is provided.
 * 3. Identifies and counts invalid records according to the 5 rules of the assignment:
 *    - fare_amount <= 0
 *    - trip_distance <= 0
 *    - passenger_count == 0 or > 6 (or null)
 *    - dropoff <= pickup (or nulls)
 *    - PULocationID or DOLocationID outside [1, 263]
 * 4. Reports cleaning statistics per year.
 * 5. Returns the cleaned DataFrame.
 */
class M1V0Baseline : DataLoader {

    override fun load(spark: SparkSession, dataPath: String, sampleFraction: Double?): Dataset<Row> {
        println("\n[M1] A carregar dados de: $dataPath")
        val dfRaw = spark.read().parquet(dataPath)

        var df = dfRaw
            .withColumn("tpep_pickup_datetime", sf.col("tpep_pickup_datetime").cast(DataTypes.TimestampType))
            .withColumn("tpep_dropoff_datetime", sf.col("tpep_dropoff_datetime").cast(DataTypes.TimestampType))
            .withColumn("passenger_count", sf.col("passenger_count").cast(DataTypes.IntegerType))
            .withColumn("RatecodeID", sf.col("RatecodeID").cast(DataTypes.IntegerType))
            .withColumn("payment_type", sf.col("payment_type").cast(DataTypes.IntegerType))
            .withColumn("PULocationID", sf.col("PULocationID").cast(DataTypes.IntegerType))
            .withColumn("DOLocationID", sf.col("DOLocationID").cast(DataTypes.IntegerType))
            .withColumn("year", sf.year(sf.col("tpep_pickup_datetime")))

        if (sampleFraction != null && sampleFraction > 0.0 && sampleFraction < 1.0) {
            println(String.format("[M1] A aplicar amostragem de %.1f%%", sampleFraction * 100.0))
            df = df.sample(sampleFraction, 42L)
        }

        val totalRecords = df.count()
        println(String.format("[M1] Total de registos lidos: %,d", totalRecords))

        // Invalidity criteria (M1)
        val condInvalidFare = sf.col("fare_amount").leq(0).or(sf.col("fare_amount").isNull)
        val condInvalidDistance = sf.col("trip_distance").leq(0).or(sf.col("trip_distance").isNull)
        val condInvalidPassenger = sf.col("passenger_count").leq(0)
            .or(sf.col("passenger_count").gt(6))
            .or(sf.col("passenger_count").isNull)
        val condInvalidTime = sf.col("tpep_dropoff_datetime").leq(sf.col("tpep_pickup_datetime"))
            .or(sf.col("tpep_pickup_datetime").isNull)
            .or(sf.col("tpep_dropoff_datetime").isNull)
        val condInvalidLocation = sf.not(sf.col("PULocationID").between(1, 263))
            .or(sf.not(sf.col("DOLocationID").between(1, 263)))
            .or(sf.col("PULocationID").isNull)
            .or(sf.col("DOLocationID").isNull)

        // Add indicator columns (0/1) to count records per criterion
        val dfFlagged = df
            .withColumn("rem_fare", sf.`when`(condInvalidFare, 1).otherwise(0))
            .withColumn("rem_distance", sf.`when`(condInvalidDistance, 1).otherwise(0))
            .withColumn("rem_passenger", sf.`when`(condInvalidPassenger, 1).otherwise(0))
            .withColumn("rem_time", sf.`when`(condInvalidTime, 1).otherwise(0))
            .withColumn("rem_location", sf.`when`(condInvalidLocation, 1).otherwise(0))

        // Per-year removal statistics per criterion (M1 requirement)
        println("\n--- [M1] Estatísticas de Limpeza por Ano (Registos Removidos) ---")
        val cleaningStats = dfFlagged
            .groupBy("year")
            .agg(
                sf.count("*").alias("total_records"),
                sf.sum("rem_fare").alias("invalid_fare"),
                sf.sum("rem_distance").alias("invalid_distance"),
                sf.sum("rem_passenger").alias("invalid_passenger"),
                sf.sum("rem_time").alias("invalid_time"),
                sf.sum("rem_location").alias("invalid_location")
            )
            .orderBy("year")

        cleaningStats.show(false)

        // Keep only the strictly valid records
        val condValidAll = sf.not(condInvalidFare)
            .and(sf.not(condInvalidDistance))
            .and(sf.not(condInvalidPassenger))
            .and(sf.not(condInvalidTime))
            .and(sf.not(condInvalidLocation))

        val dfClean = dfFlagged
            .filter(condValidAll)
            .drop("rem_fare", "rem_distance", "rem_passenger", "rem_time", "rem_location")

        val validRecords = dfClean.count()
        val removedRecords = totalRecords - validRecords
        val pctRemoved = if (totalRecords > 0) (removedRecords.toDouble() / totalRecords.toDouble() * 100.0) else 0.0
        println(String.format("[M1] Registos válidos: %,d (%.2f%%)", validRecords, 100.0 - pctRemoved))
        println(String.format("[M1] Registos removidos no total: %,d (%.2f%%)", removedRecords, pctRemoved))

        return dfClean
    }
}
