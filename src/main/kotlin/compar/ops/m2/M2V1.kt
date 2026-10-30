package compar.ops.m2

import compar.ops.Analysis
import compar.ops.AnalysisInput
import compar.ops.Op
import compar.ops.VersionInfo
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

/**
 * M2 v1 — TODO: nome da otimização.
 * Bottleneck (v0): TODO: o que o profiling mostrou.
 * Alteração: TODO: o que muda face à v0.
 */
class M2V1 : Analysis {
    override val info = VersionInfo(
        Op.M2, 1,
        bottleneck = "TODO",
        change = "TODO"
    )

    override fun run(input: AnalysisInput): Dataset<Row> {
        // TODO: copiar M2V0Baseline e aplicar a otimização
        TODO("M2 v1 por implementar")
    }
}
