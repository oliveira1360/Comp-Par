package compar.ops.o4

import compar.ops.Analysis
import compar.ops.AnalysisInput
import compar.ops.Op
import compar.ops.VersionInfo
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

/**
 * O4 v1 — TODO: nome da otimização.
 * Bottleneck (v0): TODO: o que o profiling mostrou.
 * Alteração: TODO: o que muda face à v0.
 */
class O4V1(
    private val airportThreshold: Double = 0.10,
    private val groupThreshold: Double = 0.08,
    private val commercialMaxGroup: Double = 0.04
) : Analysis {
    override val info = VersionInfo(
        Op.O4, 1,
        bottleneck = "TODO",
        change = "TODO"
    )

    override fun run(input: AnalysisInput): Dataset<Row> {
        // TODO: copiar O4V0Baseline e aplicar a otimização
        TODO("O4 v1 por implementar")
    }
}
