package compar.ops.o4

import compar.ops.Analysis
import compar.ops.AnalysisInput
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

/**
 * O4 v1 — TODO: optimization name.
 * Bottleneck (v0): TODO: what profiling showed.
 * Change: TODO: what changes compared to v0.
 */
class O4V1(
    private val airportThreshold: Double = 0.10,
    private val groupThreshold: Double = 0.08,
    private val commercialMaxGroup: Double = 0.04
) : Analysis {
    override fun run(input: AnalysisInput): Dataset<Row> {
        // TODO: copy O4V0Baseline and apply the optimization
        TODO("O4 v1 por implementar")
    }
}
