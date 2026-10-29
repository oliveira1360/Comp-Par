package compar

import compar.config.CliArgs
import compar.config.parseArgs
import compar.processor.AiGeneratedDataProcessor
import compar.processor.DataProcessor
import compar.profiler.SparkProfiler
import compar.util.SparkUtils
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

/**
 * Main.kt - Ponto de entrada do pipeline Spark em Kotlin para Análise dos Dados de NYC Yellow Taxi.
 * Computação Paralela (ComPar 26/27)
 *
 * Arquitetura desacoplada:
 * - Configuração CLI via `compar.config.parseArgs`
 * - Operações analíticas (M1, M2, M3, O1, O4) desacopladas através da interface `DataProcessor`
 * - Implementação Baseline gerada por IA (`AiGeneratedDataProcessor`)
 * - Perfilagem de alta fidelidade via `SparkProfiler`
 */
fun main(args: Array<String>) {
    val cliArgs = parseArgs(args)

    val spark = SparkUtils.getSparkSession(masterUrl = cliArgs.master)
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

    val profiler = if (cliArgs.profile) SparkProfiler(spark) else null
    val processor: DataProcessor = AiGeneratedDataProcessor()

    val startTimeNs = System.nanoTime()

    // Carregamento da tabela dimensional de zonas (lookup)
    val zonesDf = SparkUtils.loadZoneLookup(spark, cliArgs.lookup)

    // Variável para armazenar o DataFrame limpo resultante de M1
    var dfClean: Dataset<Row>? = null

    // 1. M1 — Ingestão e Limpeza de Dados
    if ("M1" in cliArgs.operations) {
        dfClean = if (profiler != null) {
            profiler.profile("M1_ingestion_and_cleaning") {
                processor.runM1IngestionAndCleaning(spark, cliArgs.data, sampleFraction = cliArgs.sample)
            }
        } else {
            processor.runM1IngestionAndCleaning(spark, cliArgs.data, sampleFraction = cliArgs.sample)
        }
    }

    // Se M1 não foi executado na lista de operações, lemos os dados diretamente para as operações seguintes
    if (dfClean == null) {
        println("[Aviso] M1 não selecionado. A carregar dados brutos diretamente para suportar as restantes etapas...")
        dfClean = processor.runM1IngestionAndCleaning(spark, cliArgs.data, sampleFraction = cliArgs.sample)
    }

    // 2. M2 — Análise de Procura Temporal
    if ("M2" in cliArgs.operations) {
        if (profiler != null) {
            profiler.profile("M2_temporal_demand") {
                processor.runM2TemporalDemandAnalysis(dfClean, zonesDf, cliArgs.output)
            }
        } else {
            processor.runM2TemporalDemandAnalysis(dfClean, zonesDf, cliArgs.output)
        }
    }

    // 3. M3 — Estatísticas ao Nível da Zona
    if ("M3" in cliArgs.operations) {
        if (profiler != null) {
            profiler.profile("M3_zone_statistics") {
                processor.runM3ZoneLevelStatistics(dfClean, zonesDf)
            }
        } else {
            processor.runM3ZoneLevelStatistics(dfClean, zonesDf)
        }
    }

    // 4. O1 — Deteção de Anomalias de Tarifa (Opcional)
    if ("O1" in cliArgs.operations) {
        if (profiler != null) {
            profiler.profile("O1_fare_anomaly_detection") {
                processor.runO1FareAnomalyDetection(dfClean, zonesDf)
            }
        } else {
            processor.runO1FareAnomalyDetection(dfClean, zonesDf)
        }
    }

    // 5. O4 — Classificação de Tipo de Viagem (Opcional)
    if ("O4" in cliArgs.operations) {
        if (profiler != null) {
            profiler.profile("O4_trip_type_classification") {
                processor.runO4TripTypeClassification(dfClean)
            }
        } else {
            processor.runO4TripTypeClassification(dfClean)
        }
    }

    val elapsedSeconds = (System.nanoTime() - startTimeNs) / 1_000_000_000.0
    println(String.format("\nTempo total decorrido do pipeline: %.2fs", elapsedSeconds))

    // Apresentação e exportação do relatório de profiling
    if (profiler != null) {
        println("\n================================================================================")
        println("  Relatório de Desempenho (SparkProfiler)")
        println("================================================================================")
        try {
            println(profiler.report())
        } catch (e: Exception) {
            println("Não foi possível gerar tabela de profiling: ${e.message}")
        }

        if (!cliArgs.exportMetricsPath.isNullOrBlank()) {
            try {
                profiler.exportJson(cliArgs.exportMetricsPath)
            } catch (e: Exception) {
                System.err.println("Erro ao exportar métricas JSON: ${e.message}")
            }
        }
    }

    spark.stop()
    println("\nSessão Spark terminada com sucesso.")
}
