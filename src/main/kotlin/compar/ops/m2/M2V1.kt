package compar.ops.m2

import compar.ops.Analysis
import compar.ops.AnalysisInput
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

/**
 * M2 v1 — TODO: optimization name.
 * Bottleneck (v0): TODO: what profiling showed.
 * Change: TODO: what changes compared to v0.
 */
class M2V1 : Analysis {
    override fun run(input: AnalysisInput): Dataset<Row> {
        // TODO: copy M2V0Baseline and apply the optimization
        TODO("M2 v1 por implementar")
    }
}
