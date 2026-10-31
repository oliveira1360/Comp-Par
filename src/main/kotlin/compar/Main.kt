package compar

import compar.config.CliArgs
import compar.config.parseArgs
import compar.ops.m1.*
import compar.ops.m2.*
import compar.ops.m3.*
import compar.ops.o1.*
import compar.ops.o4.*
import compar.profiler.SparkProfiler
import compar.runner.PipelineRunner
import compar.util.SparkUtils
import org.apache.hadoop.shaded.org.checkerframework.checker.units.qual.m2
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
            m1 = M1V3(),
            m2 = M2V3(),
            m3 = M3V0Baseline(),
            o1 = O1V0Baseline(),
            o4 = O4V0Baseline()
        )

        val startTimeNs = System.nanoTime()
        runner.run()
        val elapsedSeconds = (System.nanoTime() - startTimeNs) / 1_000_000_000.0
        println(String.format("\nTotal pipeline execution time: %.2fs", elapsedSeconds))

        printAndExportReport(profiler, cliArgs.exportMetricsPath)
    } finally {
        spark.stop()
        println("\nSpark session closed successfully.")
    }
}

/**
 * Prints the header with the run configuration.
 */
private fun printBanner(spark: SparkSession, cliArgs: CliArgs) {
    println("================================================================================")
    println("  NYC TLC Yellow Taxi Analytics Pipeline (ComPar 26/27)")
    println("================================================================================")
    println("Master:              ${spark.sparkContext().master()}")
    println("Default Parallelism: ${spark.sparkContext().defaultParallelism()}")
    val uiWebUrl = spark.sparkContext().uiWebUrl()
    if (uiWebUrl.isDefined) {
        println("Spark Web UI:        ${uiWebUrl.get()}")
    }
    println("Selected Operations: ${cliArgs.operations.joinToString(", ")}")
    println("Input Data File:     ${cliArgs.data}")
    println("================================================================================\n")
}

/**
 * Prints the metrics table and exports the profiler results as JSON.
 */
private fun printAndExportReport(profiler: SparkProfiler?, exportPath: String?) {
    if (profiler == null) return

    println("\n================================================================================")
    println("  Performance Engineering Report (SparkProfiler)")
    println("================================================================================")
    try {
        println(profiler.report())
    } catch (e: Exception) {
        println("Could not generate profiling report table: ${e.message}")
    }

    // Displays stage details if any run experienced significant task skew (>= 3.0x)
    val skewedRuns = profiler.runs.filter { (_, m) ->
        m.stages.any { it.medianTaskDurationMs > 0 && it.maxTaskDurationMs >= 3 * it.medianTaskDurationMs }
    }
    if (skewedRuns.isNotEmpty()) {
        println("\n[Warning] [SparkProfiler] Significant task skew detected (Task Skew >= 3.0x):")
        for ((name, _) in skewedRuns) {
            println("\n--- Stage Breakdown for $name ---")
            println(profiler.stageBreakdown(name))
        }
    }

    if (!exportPath.isNullOrBlank()) {
        try {
            profiler.exportJson(exportPath)
        } catch (e: Exception) {
            System.err.println("Error exporting JSON metrics: ${e.message}")
        }
    }
}
