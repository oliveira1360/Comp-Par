package compar.ops.m1

import compar.ops.Ingestion
import compar.ops.Op
import compar.ops.Source
import compar.ops.VersionInfo
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions as sf
import org.apache.spark.sql.types.DataTypes

/**
 * M1 — Ingestão e Limpeza de Dados:
 * 1. Carrega os Parquets de viagens com cast explícito de tipos.
 * 2. Aplica amostragem se sampleFraction for fornecido.
 * 3. Identifica e contabiliza registos inválidos segundo as 5 regras do enunciado:
 *    - fare_amount <= 0
 *    - trip_distance <= 0
 *    - passenger_count == 0 ou > 6 (ou nulo)
 *    - dropoff <= pickup (ou nulos)
 *    - PULocationID ou DOLocationID fora de [1, 263]
 * 4. Reporta estatísticas de limpeza por ano.
 * 5. Retorna o DataFrame limpo.
 */
class M1V0Baseline : Ingestion {
    override val info = VersionInfo(Op.M1, 0, change = "Baseline gerado por IA")

    override fun load(spark: SparkSession, source: Source): Dataset<Row> {
        println("\n[${info.label}] A carregar dados de: ${source.dataPath}")
        val dfRaw = spark.read().parquet(source.dataPath)

        var df = dfRaw
            .withColumn("tpep_pickup_datetime", sf.col("tpep_pickup_datetime").cast(DataTypes.TimestampType))
            .withColumn("tpep_dropoff_datetime", sf.col("tpep_dropoff_datetime").cast(DataTypes.TimestampType))
            .withColumn("passenger_count", sf.col("passenger_count").cast(DataTypes.IntegerType))
            .withColumn("RatecodeID", sf.col("RatecodeID").cast(DataTypes.IntegerType))
            .withColumn("payment_type", sf.col("payment_type").cast(DataTypes.IntegerType))
            .withColumn("PULocationID", sf.col("PULocationID").cast(DataTypes.IntegerType))
            .withColumn("DOLocationID", sf.col("DOLocationID").cast(DataTypes.IntegerType))
            .withColumn("year", sf.year(sf.col("tpep_pickup_datetime")))

        val sampleFraction = source.sampleFraction
        if (sampleFraction != null && sampleFraction > 0.0 && sampleFraction < 1.0) {
            println(String.format("[M1] A aplicar amostragem de %.1f%%", sampleFraction * 100.0))
            df = df.sample(sampleFraction, 42L)
        }

        val totalRecords = df.count()
        println(String.format("[M1] Total de registos lidos: %,d", totalRecords))

        // Definição dos critérios de invalidade (M1)
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

        // Adicionar colunas indicadoras (0/1) para contabilização por critério
        val dfFlagged = df
            .withColumn("rem_fare", sf.`when`(condInvalidFare, 1).otherwise(0))
            .withColumn("rem_distance", sf.`when`(condInvalidDistance, 1).otherwise(0))
            .withColumn("rem_passenger", sf.`when`(condInvalidPassenger, 1).otherwise(0))
            .withColumn("rem_time", sf.`when`(condInvalidTime, 1).otherwise(0))
            .withColumn("rem_location", sf.`when`(condInvalidLocation, 1).otherwise(0))

        // Estatísticas por ano de remoção por critério (requisito M1)
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

        // Filtrar o DataFrame apenas com os registos estritamente válidos
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
