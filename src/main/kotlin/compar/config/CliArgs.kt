package compar.config

/**
 * Representa os argumentos da linha de comando para o pipeline de análise NYC Yellow Taxi.
 */
data class CliArgs(
    val data: String = "data/yellow_tripdata_2024-01.parquet",
    val lookup: String = "data/taxi_zone_lookup.csv",
    val output: String = "output",
    val master: String? = null,
    val sample: Double? = null,
    val profile: Boolean = false,
    val operations: List<String> = listOf("M1", "M2", "M3", "O1", "O4"),
    val exportMetricsPath: String? = "output/profiling_report.json"
)

fun parseArgs(args: Array<String>): CliArgs {
    var data = "data/yellow_tripdata_2024-01.parquet"
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
                require(i + 1 < args.size) { "O parâmetro --data requer um valor." }
                data = args[++i]
            }
            "--lookup" -> {
                require(i + 1 < args.size) { "O parâmetro --lookup requer um valor." }
                lookup = args[++i]
            }
            "--output" -> {
                require(i + 1 < args.size) { "O parâmetro --output requer um valor." }
                output = args[++i]
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
            "--ops", "--operations" -> {
                require(i + 1 < args.size) { "O parâmetro --ops requer uma lista separada por vírgulas (ex: M1,M2,O1)." }
                operations = args[++i].split(",").map { it.trim().uppercase() }.filter { it.isNotEmpty() }
            }
            "--export-metrics" -> {
                require(i + 1 < args.size) { "O parâmetro --export-metrics requer um caminho de ficheiro." }
                exportMetricsPath = args[++i]
            }
            "--help", "-h" -> {
                println(
                    """
                    Uso: spark-submit ... nyc-taxi-analytics.jar [opções]
                      --data <path>           Caminho para ficheiro(s) Parquet de entrada (default: data/yellow_tripdata_2024-01.parquet)
                      --lookup <path>         Caminho para o ficheiro taxi_zone_lookup.csv (default: data/taxi_zone_lookup.csv)
                      --output <path>         Diretoria de saída para resultados Parquet (default: output)
                      --master <url>          URL do Spark Master (ex: local[*] ou spark://spark-master:7077)
                      --sample <fraction>     Fração de amostragem dos dados (ex: 0.1 para 10%)
                      --profile               Ativar o SparkProfiler para recolha de métricas de stages/shuffles
                      --ops <M1,M2,M3,O1,O4>  Operações a executar (default: M1,M2,M3,O1,O4)
                      --export-metrics <path> Caminho JSON para exportar métricas do profiler (default: output/profiling_report.json)
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
