package compar.runner

import compar.config.CliArgs
import compar.processor.DataProcessor
import compar.profiler.SparkProfiler
import compar.util.SparkUtils
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

/**
 * PipelineRunner é responsável por orquestrar a execução das operações analíticas (M1-M3, O1, O4),
 * gerir o ciclo de vida dos DataFrames em memória (dfClean, zonesDf) e aplicar profiling
 * de forma transparente a cada etapa solicitada.
 */
class PipelineRunner(
    private val spark: SparkSession,
    private val cliArgs: CliArgs,
    private val processor: DataProcessor,
    private val profiler: SparkProfiler?
) {
    // Cache local dos dados limpos de M1; apenas computado na primeira vez que for necessário
    private var dfClean: Dataset<Row>? = null

    // Tabela dimensional de zonas; carregada preguiçosamente apenas se uma operação precisar dela
    private val zonesDf by lazy { SparkUtils.loadZoneLookup(spark, cliArgs.lookup) }

    /**
     * Executa em sequência as operações especificadas em cliArgs.operations.
     */
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

    /**
     * Obtém os dados limpos de M1. Se M1 ainda não tiver sido executado,
     * executa-o automaticamente para garantir a integridade das operações seguintes.
     */
    private fun getCleanData(): Dataset<Row> {
        if (dfClean == null) {
            dfClean = runOp("M1_ingestion_and_cleaning") {
                processor.runM1IngestionAndCleaning(spark, cliArgs.data, sampleFraction = cliArgs.sample)
            }
        }
        return dfClean!!
    }

    /**
     * Executa um bloco de código com profiling se o profiler estiver ativo,
     * ou diretamente caso contrário.
     */
    private fun <T> runOp(name: String, block: () -> T): T =
        profiler?.profile(name, block) ?: block()
}
