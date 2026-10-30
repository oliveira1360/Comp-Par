package compar.runner

import compar.config.CliArgs
import compar.ops.AnalysisInput
import compar.ops.Op
import compar.ops.Registry
import compar.ops.Source
import compar.ops.Step
import compar.profiler.SparkProfiler
import compar.util.SparkUtils
import org.apache.spark.sql.SparkSession

/**
 * PipelineRunner é responsável por orquestrar a execução das operações analíticas (M1-M3, O1, O4):
 * resolve no Registry a versão pedida de cada operação, executa o M1 uma única vez
 * e aplica profiling de forma transparente a cada etapa.
 */
class PipelineRunner(
    private val spark: SparkSession,
    private val cliArgs: CliArgs,
    private val profiler: SparkProfiler?
) {
    /**
     * Executa o M1 e, em sequência, as análises especificadas em cliArgs.operations.
     * O M1 corre sempre, pois todas as análises dependem dos dados limpos.
     */
    fun run() {
        val ingestion = Registry.ingestion(versionOf(Op.M1))
        val analyses = cliArgs.operations
            .filter { it != Op.M1 }
            .map { Registry.analysis(it, versionOf(it)) }

        val trips = profiled(ingestion) {
            ingestion.load(spark, Source(cliArgs.data, cliArgs.sample))
        }
        if (analyses.isEmpty()) return

        // Valida as colunas pedidas por cada análise antes de executar qualquer uma delas
        val available = trips.columns().toSet()
        analyses.forEach { analysis ->
            val missing = analysis.requiredColumns - available
            require(missing.isEmpty()) {
                "${analysis.info.label} precisa das colunas $missing, que ${ingestion.info.label} não produz"
            }
        }

        val input = AnalysisInput(
            spark = spark,
            trips = trips,
            zones = SparkUtils.loadZoneLookup(spark, cliArgs.lookup),
            outputDir = cliArgs.output
        )
        analyses.forEach { analysis -> profiled(analysis) { analysis.run(input) } }
    }

    /** Versão pedida para a operação; null significa a mais recente. */
    private fun versionOf(op: Op): Int? = cliArgs.versions[op] ?: cliArgs.defaultVersion

    /**
     * Executa um bloco de código com profiling se o profiler estiver ativo,
     * ou diretamente caso contrário.
     */
    private fun <T> profiled(step: Step, block: () -> T): T =
        profiler?.profile(step.info.label, block) ?: block()
}
