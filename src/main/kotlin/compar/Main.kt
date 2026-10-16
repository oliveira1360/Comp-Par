package compar

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions as sf
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

/**
 * Main.kt - Pipeline Spark em Kotlin para Análise dos Dados de NYC Yellow Taxi
 * Computação Paralela (ComPar 26/27)
 *
 * Estrutura inicial com foco na operação obrigatória M1 (Ingestão e Limpeza),
 * suporte a amostragem (--sample), profiling com SparkProfiler e execução
 * tanto em modo local como no cluster Docker.
 */

data class CliArgs(
    val data: String = "data/yellow_tripdata_2024-01.parquet",
    val lookup: String = "data/taxi_zone_lookup.csv",
    val master: String? = null,
    val sample: Double? = null,
    val profile: Boolean = false
)

fun parseArgs(args: Array<String>): CliArgs {
    var data = "data/yellow_tripdata_2024-01.parquet"
    var lookup = "data/taxi_zone_lookup.csv"
    var master: String? = null
    var sample: Double? = null
    var profile = false

    var i = 0
    while (i < args.size) {
        when (args[i]) {
            "--data" -> {
                require(i + 1 < args.size) { "O parâmetro --data requer um valor." }
                data = args[++i]
            }
            "--lookup" -> {
                require(i + 1 < args.size) { "O parâmetro --lookup requer um valor." }
                lookup = args[++i]
            }
            "--master" -> {
                require(i + 1 < args.size) { "O parâmetro --master requer um valor." }
                master = args[++i]
            }
            "--sample" -> {
                require(i + 1 < args.size) { "O parâmetro --sample requer um valor numérico." }
                sample = args[++i].toDoubleOrNull()
                    ?: throw IllegalArgumentException("Valor inválido para --sample: ${args[i]}")
            }
            "--profile" -> {
                profile = true
            }
            "--help", "-h" -> {
                println(
                    """
                    Uso: spark-submit ... nyc-taxi-analytics.jar [opções]
                      --data <path>       Caminho para ficheiro(s) Parquet de entrada (default: data/yellow_tripdata_2024-01.parquet)
                      --lookup <path>     Caminho para o ficheiro taxi_zone_lookup.csv (default: data/taxi_zone_lookup.csv)
                      --master <url>      URL do Spark Master (ex: local[*] ou spark://spark-master:7077)
                      --sample <fraction> Fração de amostragem dos dados (ex: 0.1 para 10%)
                      --profile           Ativar o SparkProfiler para recolha de métricas de stages/shuffles
                    """.trimIndent()
                )
                 kotlin.system.exitProcess(0)
            }
            else -> {
                System.err.println("Aviso: argumento desconhecido ignorado: ${args[i]}")
            }
        }
        i++
    }

    return CliArgs(data = data, lookup = lookup, master = master, sample = sample, profile = profile)
}

fun getSparkSession(masterUrl: String? = null, appName: String = "NYC-Taxi-Analytics"): SparkSession {
    var builder = SparkSession.builder().appName(appName)
    if (!masterUrl.isNullOrBlank()) {
        builder = builder.master(masterUrl)
    }

    // Configurações recomendadas de memória para o ambiente Docker/Local
    builder = builder
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "1536m")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.adaptive.enabled", "true")

    return builder.getOrCreate()
}

/**
 * Define o esquema explícito para os dados do TLC Yellow Taxi.
 * Evita a necessidade de inferSchema e garante tipos consistentes.
 */
fun getRawSchema(): StructType = StructType(
    arrayOf(
        StructField.apply("tpep_pickup_datetime", DataTypes.TimestampType, true, org.apache.spark.sql.types.Metadata.empty()),
        StructField.apply("tpep_dropoff_datetime", DataTypes.TimestampType, true, org.apache.spark.sql.types.Metadata.empty()),
        StructField.apply("PULocationID", DataTypes.IntegerType, true, org.apache.spark.sql.types.Metadata.empty()),
        StructField.apply("DOLocationID", DataTypes.IntegerType, true, org.apache.spark.sql.types.Metadata.empty()),
        StructField.apply("passenger_count", DataTypes.DoubleType, true, org.apache.spark.sql.types.Metadata.empty()),
        StructField.apply("trip_distance", DataTypes.DoubleType, true, org.apache.spark.sql.types.Metadata.empty()),
        StructField.apply("RatecodeID", DataTypes.DoubleType, true, org.apache.spark.sql.types.Metadata.empty()),
        StructField.apply("payment_type", DataTypes.LongType, true, org.apache.spark.sql.types.Metadata.empty()),
        StructField.apply("fare_amount", DataTypes.DoubleType, true, org.apache.spark.sql.types.Metadata.empty()),
        StructField.apply("tip_amount", DataTypes.DoubleType, true, org.apache.spark.sql.types.Metadata.empty()),
        StructField.apply("total_amount", DataTypes.DoubleType, true, org.apache.spark.sql.types.Metadata.empty())
    )
)

/**
 * M1 - Data Ingestion and Cleaning:
 * 1. Carrega os Parquets de viagens com cast explícito de tipos.
 * 2. Aplica amostragem se sampleFraction for fornecido.
 * 3. Identifica e contabiliza registos inválidos segundo as 5 regras do enunciado:
 *    - fare_amount <= 0
 *    - trip_distance <= 0
 *    - passenger_count == 0 ou > 6 (ou nulo)
 *    - dropoff <= pickup (ou nulos)
 *    - PULocationID ou DOLocationID fora de [1, 263]
 * 4. Reporta estatísticas de limpeza por ano.
 * 5. Retorna o DataFrame (Dataset<Row>) limpo.
 */
fun runM1IngestionAndCleaning(
    spark: SparkSession,
    dataPath: String,
    sampleFraction: Double? = null
): Dataset<Row> {
    println("\n[M1] A carregar dados de: $dataPath")
    val dfRaw = spark.read().parquet(dataPath)

    // Cast e seleção das colunas essenciais
    var df = dfRaw
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

fun main(args: Array<String>) {
    val cliArgs = parseArgs(args)

    val spark = getSparkSession(masterUrl = cliArgs.master)
    println("Sessão Spark iniciada com sucesso (Kotlin).")
    println("Master: ${spark.sparkContext().master()}")
    println("Paralelismo por defeito: ${spark.sparkContext().defaultParallelism()}")
    val uiWebUrl = spark.sparkContext().uiWebUrl()
    if (uiWebUrl.isDefined) {
        println("Spark UI disponível em: ${uiWebUrl.get()}")
    }

    val profiler = if (cliArgs.profile) SparkProfiler(spark) else null

    val startTimeNs = System.nanoTime()

    // Execução de M1
    val dfClean = if (profiler != null) {
        profiler.profile("M1_ingestion_and_cleaning") {
            runM1IngestionAndCleaning(spark, cliArgs.data, sampleFraction = cliArgs.sample)
        }
    } else {
        runM1IngestionAndCleaning(spark, cliArgs.data, sampleFraction = cliArgs.sample)
    }

    val elapsedSeconds = (System.nanoTime() - startTimeNs) / 1_000_000_000.0
    println(String.format("\nTempo total decorrido: %.2fs", elapsedSeconds))

    if (profiler != null) {
        println("\n--- Relatório de Profiling ---")
        try {
            println(profiler.report())
        } catch (e: Exception) {
            println("Não foi possível gerar tabela de profiling: ${e.message}")
        }
    }

    spark.stop()
    println("Sessão Spark terminada.")
}
