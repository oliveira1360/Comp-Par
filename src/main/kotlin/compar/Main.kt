package compar

import compar.config.CliArgs
import compar.config.parseArgs
import compar.processor.AiGeneratedDataProcessor
import compar.processor.DataProcessor
import compar.profiler.SparkProfiler
import compar.util.SparkUtils
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

/**
 * Main.kt - Ponto de entrada do pipeline Spark em Kotlin para Análise dos Dados de NYC Yellow Taxi.
 * Computação Paralela (ComPar 26/27)
 */
fun main(args: Array<String>) {
    val cliArgs = parseArgs(args)
    val spark = SparkUtils.getSparkSession(masterUrl = cliArgs.master)

    try {
        printBanner(spark, cliArgs)

        val profiler = if (cliArgs.profile) SparkProfiler(spark) else null
        val runner = PipelineRunner(
            spark = spark,
            cliArgs = cliArgs,
            processor = AiGeneratedDataProcessor(),
            profiler = profiler
        )

        val startTimeNs = System.nanoTime()
        runner.run()
        val elapsedSeconds = (System.nanoTime() - startTimeNs) / 1_000_000_000.0
        println(String.format("\nTempo total decorrido do pipeline: %.2fs", elapsedSeconds))

        printAndExportReport(profiler, cliArgs.exportMetricsPath)
    } finally {
        spark.stop()
        println("\nSessão Spark terminada com sucesso.")
    }
}

/**
 * Responsável pela orquestração e execução com profiling das operações solicitadas.
 */
class PipelineRunner(
    private val spark: SparkSession,
    private val cliArgs: CliArgs,
    private val processor: DataProcessor,
    private val profiler: SparkProfiler?
) {
    private var dfClean: Dataset<Row>? = null
    private val zonesDf by lazy { SparkUtils.loadZoneLookup(spark, cliArgs.lookup) }

    fun run() {
        for (op in cliArgs.operations) {
            executeOperation(op)
        }
    }

    private fun executeOperation(op: String) {
        when (op) {
            "M1" -> getCleanData()
            "M2" -> runOp("M2_temporal_demand") {
                processor.runM2TemporalDemandAnalysis(getCleanData(), zonesDf, cliArgs.output)
            }
            "M3" -> runOp("M3_zone_statistics") {
                processor.runM3ZoneLevelStatistics(getCleanData(), zonesDf)
            }
            "O1" -> runOp("O1_fare_anomaly_detection") {
                processor.runO1FareAnomalyDetection(getCleanData(), zonesDf)
            }
            "O4" -> runOp("O4_trip_type_classification") {
                processor.runO4TripTypeClassification(getCleanData())
            }
            else -> System.err.println("Aviso: operação '$op' desconhecida.")
        }
    }

    private fun getCleanData(): Dataset<Row> {
        if (dfClean == null) {
            dfClean = runOp("M1_ingestion_and_cleaning") {
                processor.runM1IngestionAndCleaning(spark, cliArgs.data, sampleFraction = cliArgs.sample)
            }
        }
        return dfClean!!
    }

    private fun <T> runOp(name: String, block: () -> T): T =
        profiler?.profile(name, block) ?: block()
}

/**
 * Apresenta o cabeçalho informativo com as configurações de execução.
 */
private fun printBanner(spark: SparkSession, cliArgs: CliArgs) {
    println("================================================================================")
    println("  NYC TLC Yellow Taxi Analytics Pipeline (ComPar 26/27)")
    println("================================================================================")
    println("Master: ${spark.sparkContext().master()}")
    println("Paralelismo por defeito: ${spark.sparkContext().defaultParallelism()}")
    val uiWebUrl = spark.sparkContext().uiWebUrl()
    if (uiWebUrl.isDefined) {
        println("Spark Web UI: ${uiWebUrl.get()}")
    }
    println("Operações selecionadas: ${cliArgs.operations.joinToString(", ")}")
    println("Ficheiro de dados: ${cliArgs.data}")
    println("================================================================================\n")
}

/**
 * Imprime a tabela de métricas e exporta os resultados do profiler em formato JSON.
 */
private fun printAndExportReport(profiler: SparkProfiler?, exportPath: String?) {
    if (profiler == null) return

    println("\n================================================================================")
    println("  Relatório de Desempenho (SparkProfiler)")
    println("================================================================================")
    try {
        println(profiler.report())
    } catch (e: Exception) {
        println("Não foi possível gerar tabela de profiling: ${e.message}")
    }

    if (!exportPath.isNullOrBlank()) {
        try {
            profiler.exportJson(exportPath)
        } catch (e: Exception) {
            System.err.println("Erro ao exportar métricas JSON: ${e.message}")
        }
    }
}
