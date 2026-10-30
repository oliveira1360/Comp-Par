package compar.ops.m1

import compar.ops.DataLoader
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

/**
 * M1 v1 — TODO: optimization name.
 * Bottleneck (v0): TODO: what profiling showed.
 * Change: TODO: what changes compared to v0.
 */
class M1V1 : DataLoader {
    override fun load(spark: SparkSession, dataPath: String, sampleFraction: Double?): Dataset<Row> {
        // TODO: copy M1V0Baseline and apply the optimization
        TODO("M1 v1 por implementar")
    }
}
