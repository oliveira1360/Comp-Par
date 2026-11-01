package compar.config

import kotlin.system.exitProcess

/**
 * Command-line arguments for the NYC Yellow Taxi analytics pipeline.
 */
data class CliArgs(
    val data: String = "data/yellow_tripdata_2015-01.parquet",
    val lookup: String = "data/taxi_zone_lookup.csv",
    val output: String = "output",
    val master: String? = null,
    val sample: Double? = null,
    val profile: Boolean = false,
    val operations: List<String> = listOf("M1", "M2", "M3", "O1", "O4"),
    val m1Version: String = "v3",
    val m2Version: String = "v7",
    val m3Version: String = "v0",
    val o1Version: String = "v0",
    val o4Version: String = "v0",
    val exportMetricsPath: String? = "output/profiling_report.json"
)

fun parseArgs(args: Array<String>): CliArgs {
    var data = "data/yellow_tripdata_2015-01.parquet"
    var lookup = "data/taxi_zone_lookup.csv"
    var output = "output"
    var master: String? = null
    var sample: Double? = null
    var profile = false
    var operations = listOf("M1", "M2", "M3", "O1", "O4")
    var m1Version = "v3"
    var m2Version = "v7"
    var m3Version = "v0"
    var o1Version = "v0"
    var o4Version = "v0"
    var exportMetricsPath: String? = "output/profiling_report.json"

    var i = 0
    while (i < args.size) {
        when (args[i]) {
            "--data" -> {
                require(i + 1 < args.size) { "Parameter --data requires a value." }
                data = args[++i]
            }
            "--lookup" -> {
                require(i + 1 < args.size) { "Parameter --lookup requires a value." }
                lookup = args[++i]
            }
            "--output" -> {
                require(i + 1 < args.size) { "Parameter --output requires a value." }
                output = args[++i]
            }
            "--master" -> {
                require(i + 1 < args.size) { "Parameter --master requires a value." }
                master = args[++i]
            }
            "--sample" -> {
                require(i + 1 < args.size) { "Parameter --sample requires a numeric value." }
                sample = args[++i].toDoubleOrNull()
                    ?: throw IllegalArgumentException("Invalid value for --sample: ${args[i]}")
            }
            "--profile" -> {
                profile = true
            }
            "--ops", "--operations" -> {
                require(i + 1 < args.size) { "Parameter --ops requires a comma-separated list (e.g. M1,M2,O1)." }
                operations = args[++i].split(",").map { it.trim().uppercase() }.filter { it.isNotEmpty() }
            }
            "--m1" -> {
                require(i + 1 < args.size) { "Parameter --m1 requires a version (e.g. v0..v3)." }
                m1Version = args[++i]
            }
            "--m2" -> {
                require(i + 1 < args.size) { "Parameter --m2 requires a version (e.g. v0..v7)." }
                m2Version = args[++i]
            }
            "--m3" -> {
                require(i + 1 < args.size) { "Parameter --m3 requires a version (e.g. v0..v1)." }
                m3Version = args[++i]
            }
            "--o1" -> {
                require(i + 1 < args.size) { "Parameter --o1 requires a version (e.g. v0..v1)." }
                o1Version = args[++i]
            }
            "--o4" -> {
                require(i + 1 < args.size) { "Parameter --o4 requires a version (e.g. v0..v1)." }
                o4Version = args[++i]
            }
            "--export-metrics" -> {
                require(i + 1 < args.size) { "Parameter --export-metrics requires a file path." }
                exportMetricsPath = args[++i]
            }
            "--help", "-h" -> {
                println(
                    """
                    Usage: spark-submit ... nyc-taxi-analytics.jar [options]
                      --data <path>           Path to input Parquet file(s) (default: data/yellow_tripdata_2015-01.parquet)
                      --lookup <path>         Path to taxi_zone_lookup.csv (default: data/taxi_zone_lookup.csv)
                      --output <path>         Output directory for Parquet results (default: output)
                      --master <url>          Spark Master URL (e.g. local[*] or spark://spark-master:7077)
                      --sample <fraction>     Sampling fraction (e.g. 0.1 for 10%)
                      --profile               Enable SparkProfiler for stage, shuffle, and memory metrics
                      --ops <M1,M2,M3,O1,O4>  Operations to execute (default: M1,M2,M3,O1,O4)
                      --m1 <version>          M1 implementation version (v0..v3, default: v3)
                      --m2 <version>          M2 implementation version (v0..v7, default: v7)
                      --m3 <version>          M3 implementation version (v0..v1, default: v0)
                      --o1 <version>          O1 implementation version (v0..v7, default: v0)
                      --o4 <version>          O4 implementation version (v0..v1, default: v0)
                      --export-metrics <path> JSON file path to export profiler metrics (default: output/profiling_report.json)
                    """.trimIndent()
                )
                exitProcess(0)
            }
            else -> {
                System.err.println("Warning: unknown argument ignored: ${args[i]}")
            }
        }
        i++
    }

    return CliArgs(
        data = data,
        lookup = lookup,
        output = output,
        master = master,
        sample = sample,
        profile = profile,
        operations = operations,
        m1Version = m1Version,
        m2Version = m2Version,
        m3Version = m3Version,
        o1Version = o1Version,
        o4Version = o4Version,
        exportMetricsPath = exportMetricsPath
    )
}
