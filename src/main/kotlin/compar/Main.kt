package compar

import compar.config.CliArgs
import compar.config.parseArgs
import compar.ops.Registry
import compar.profiler.SparkProfiler
import compar.runner.PipelineRunner
import compar.util.SparkUtils
import org.apache.spark.sql.SparkSession

/**
 * Main.kt - Ponto de entrada do pipeline Spark em Kotlin para Análise dos Dados de NYC Yellow Taxi.
 * Computação Paralela (ComPar 26/27)
 */
fun main(args: Array<String>) {
    val cliArgs = parseArgs(args)
    if (cliArgs.listVersions) {
        printVersionTable()
        return
    }

    val spark = SparkUtils.getSparkSession(masterUrl = cliArgs.master)
    try {
        printBanner(spark, cliArgs)

        val profiler = if (cliArgs.profile) SparkProfiler(spark) else null
        val runner = PipelineRunner(
            spark = spark,
            cliArgs = cliArgs,
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
    val requested = cliArgs.versions.entries.joinToString(", ") { "${it.key}=v${it.value}" }
    val fallback = cliArgs.defaultVersion?.let { "v$it" } ?: "mais recente"
    println("Versões: ${requested.ifEmpty { "-" }} (restantes: $fallback)")
    println("Ficheiro de dados: ${cliArgs.data}")
    println("================================================================================\n")
}

/**
 * Lista todas as versões registadas, com o bottleneck e a alteração de cada uma.
 */
private fun printVersionTable() {
    println(String.format("%-4s %-4s %-60s %s", "Op", "Ver", "Bottleneck (versão anterior)", "Alteração"))
    println(String.format("%-4s %-4s %-60s %s", "--", "---", "-".repeat(28), "-".repeat(9)))
    for (step in Registry.all()) {
        val info = step.info
        println(String.format("%-4s %-4d %-60s %s", info.op, info.version, info.bottleneck, info.change))
    }
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
