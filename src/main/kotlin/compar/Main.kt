package compar

import compar.config.CliArgs
import compar.config.parseArgs
import compar.ops.Analysis
import compar.ops.DataLoader
import compar.ops.m1.*
import compar.ops.m2.*
import compar.ops.m3.*
import compar.ops.o1.*
import compar.ops.o4.*
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
    val m1 = createM1(cliArgs.m1Version)
    val m2 = createM2(cliArgs.m2Version)
    val m3 = createM3(cliArgs.m3Version)
    val o1 = createO1(cliArgs.o1Version)
    val o4 = createO4(cliArgs.o4Version)

    val spark = SparkUtils.getSparkSession(masterUrl = cliArgs.master)
    try {
        printBanner(spark, cliArgs, listOf(m1.tag, m2.tag, m3.tag, o1.tag, o4.tag))

        val profiler = if (cliArgs.profile) SparkProfiler(spark) else null
        val runner = PipelineRunner(
            spark = spark,
            cliArgs = cliArgs,
            profiler = profiler,
            m1 = m1,
            m2 = m2,
            m3 = m3,
            o1 = o1,
            o4 = o4
        )

        val startTimeNs = System.nanoTime()
        runner.run()
        val elapsedSeconds = (System.nanoTime() - startTimeNs) / 1_000_000_000.0
        println(String.format("\nTotal pipeline execution time: %.2fs", elapsedSeconds))

        profiler?.printSummary(cliArgs.exportMetricsPath)
    } finally {
        spark.stop()
        println("\nSpark session closed successfully.")
    }
}

private fun normalizeVersion(v: String): String {
    val clean = v.trim().lowercase()
        .removePrefix("m1").removePrefix("m2").removePrefix("m3")
        .removePrefix("o1").removePrefix("o4")
        .removeSuffix("baseline")
    return if (clean.all { it.isDigit() } && clean.isNotEmpty()) "v$clean" else clean
}

private fun createM1(v: String): DataLoader = when (normalizeVersion(v)) {
    "v0", "base", "" -> M1V0Baseline()
    "v1" -> M1V1()
    "v2" -> M1V2()
    "v3" -> M1V3()
    else -> throw IllegalArgumentException("Unknown M1 version '$v' (available: v0..v3)")
}

private fun createM2(v: String): Analysis = when (normalizeVersion(v)) {
    "v0", "base", "" -> M2V0Baseline()
    "v1" -> M2V1()
    "v2" -> M2V2()
    "v3" -> M2V3()
    "v4" -> M2V4()
    "v5" -> M2V5()
    "v6" -> M2V6()
    "v7" -> M2V7()
    else -> throw IllegalArgumentException("Unknown M2 version '$v' (available: v0..v7)")
}

private fun createM3(v: String): Analysis = when (normalizeVersion(v)) {
    "v0", "base", "" -> M3V0Baseline()
    "v1" -> M3V1()
    else -> throw IllegalArgumentException("Unknown M3 version '$v' (available: v0..v1)")
}

private fun createO1(v: String): Analysis = when (normalizeVersion(v)) {
    "v0", "base", "" -> O1V0Baseline()
    "v1" -> O1V1()
    "v2" -> O1V2()
    "v3" -> O1V3()
    "v4" -> O1V4()
    "v5" -> O1V5()
    "v6" -> O1V6()
    "v7" -> O1V7()
    else -> throw IllegalArgumentException("Unknown O1 version '$v' (available: v0..v7)")
}

private fun createO4(v: String): Analysis = when (normalizeVersion(v)) {
    "v0", "base", "" -> O4V0Baseline()
    "v1" -> O4V1()
    else -> throw IllegalArgumentException("Unknown O4 version '$v' (available: v0..v1)")
}

/**
 * Prints the header with the run configuration.
 */
private fun printBanner(spark: SparkSession, cliArgs: CliArgs, activeTags: List<String>) {
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
    println("Active Versions:     ${activeTags.joinToString(", ")}")
    println("Input Data File:     ${cliArgs.data}")
    println("================================================================================\n")
}
