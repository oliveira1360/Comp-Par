package compar.runner

import compar.config.CliArgs
import compar.ops.Analysis
import compar.ops.AnalysisInput
import compar.ops.DataLoader
import compar.profiler.SparkProfiler
import compar.util.SparkUtils
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

/**
 * PipelineRunner orchestrates the execution of the analytical operations (M1-M3, O1, O4),
 * manages the lifecycle of the in-memory DataFrames and transparently applies profiling
 * to each requested step. Receives the implementation of each operation through dependency injection.
 */
class PipelineRunner(
    private val spark: SparkSession,
    private val cliArgs: CliArgs,
    private val profiler: SparkProfiler?,
    private val m1: DataLoader,
    private val m2: Analysis,
    private val m3: Analysis,
    private val o1: Analysis,
    private val o4: Analysis
) {
    // Cleaned M1 data; M1 runs automatically the first time it is needed, and only once
    private val dfClean: Dataset<Row> by lazy {
        runOp("M1_ingestion_and_cleaning") {
            m1.load(spark, cliArgs.data, cliArgs.sample)
        }
    }

    // Input of the analyses; loads the zone table only if an operation needs it
    private val input by lazy {
        AnalysisInput(spark, dfClean, SparkUtils.loadZoneLookup(spark, cliArgs.lookup), cliArgs.output)
    }

    /**
     * Runs the operations specified in cliArgs.operations in sequence.
     */
    fun run() {
        // Resolve dfClean first so that nested M1 profiling does not clear the JobGroup of downstream operations
        dfClean
        for (op in cliArgs.operations) {
            executeOperation(op)
        }
    }

    private fun executeOperation(op: String) {
        when (op) {
            "M1" -> dfClean
            "M2" -> runOp("M2_temporal_demand") { m2.run(input) }
            "M3" -> runOp("M3_zone_statistics") { m3.run(input) }
            "O1" -> runOp("O1_fare_anomaly_detection") { o1.run(input) }
            "O4" -> runOp("O4_trip_type_classification") { o4.run(input) }
            else -> System.err.println("Warning: unknown operation '$op'.")
        }
    }

    /**
     * Runs a code block with profiling if the profiler is active,
     * or directly otherwise.
     */
    private fun <T> runOp(name: String, block: () -> T): T =
        profiler?.profile(name, block) ?: block()
}