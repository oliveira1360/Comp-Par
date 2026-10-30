package compar.ops.m1

import compar.ops.Ingestion
import compar.ops.Op
import compar.ops.Source
import compar.ops.VersionInfo
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

/**
 * M1 v1 — TODO: nome da otimização.
 * Bottleneck (v0): TODO: o que o profiling mostrou.
 * Alteração: TODO: o que muda face à v0.
 */
class M1V1 : Ingestion {
    override val info = VersionInfo(
        Op.M1, 1,
        bottleneck = "TODO",
        change = "TODO"
    )

    override fun load(spark: SparkSession, source: Source): Dataset<Row> {
        // TODO: copiar M1V0Baseline e aplicar a otimização
        TODO("M1 v1 por implementar")
    }
}
