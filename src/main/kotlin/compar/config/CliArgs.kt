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
        exportMetricsPath = exportMetricsPath
    )
}
