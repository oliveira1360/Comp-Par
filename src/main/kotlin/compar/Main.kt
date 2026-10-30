package compar

import compar.config.CliArgs
import compar.config.parseArgs
import compar.ops.m1.M1V0Baseline
import compar.ops.m2.M2V0Baseline
import compar.ops.m3.M3V0Baseline
import compar.ops.o1.O1V0Baseline
import compar.ops.o4.O4V0Baseline
import compar.profiler.SparkProfiler
import compar.runner.PipelineRunner
import compar.util.SparkUtils
import org.apache.spark.sql.SparkSession

/**
 * Main.kt - Entry point of the Kotlin Spark pipeline for NYC Yellow Taxi data analysis.
 * Parallel Computing (ComPar 26/27)
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
            profiler = profiler,
            m1 = M1V0Baseline(),
            m2 = M2V0Baseline(),
            m3 = M3V0Baseline(),
            o1 = O1V0Baseline(),
            o4 = O4V0Baseline()
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
 * Prints the header with the run configuration.
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
 * Prints the metrics table and exports the profiler results as JSON.
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
