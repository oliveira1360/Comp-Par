package compar.ops.m3

import compar.ops.Analysis
import compar.ops.AnalysisInput
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

/**
 * M3 v1 — TODO: optimization name.
 * Bottleneck (v0): TODO: what profiling showed.
 * Change: TODO: what changes compared to v0.
 */
class M3V1 : Analysis {
    override fun run(input: AnalysisInput): Dataset<Row> {
        // TODO: copy M3V0Baseline and apply the optimization
        TODO("M3 v1 por implementar")
    }
}
