package compar.ops.m2

import compar.ops.Analysis
import compar.ops.AnalysisInput
import compar.ops.common.pickupZones
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions as sf

/**
 * M2 v2 — partition-aligned Parquet write.
 * Bottleneck (v1): writing with partitionBy("pickup_borough") after a global orderBy leaves rows
 * of every borough scattered across partitions, forcing each task to open multiple Parquet writers
 * simultaneously and triggering an extra range-sampling stage for orderBy.
 * Change: replace the global orderBy with repartition(col("pickup_borough")) before writing so that
 * each borough is written by a single worker task.
 */
class M2V2 : Analysis {

    override fun run(input: AnalysisInput): Dataset<Row> {
        val tag = javaClass.simpleName
        println("\n--- [$tag] Temporal Demand Analysis ---")

        val m2Df = input.trips
            .select(*m2TripColumns)
            .join(sf.broadcast(pickupZones(input.zones)), "PULocationID")
            .let(::aggregateTemporalDemand)
            .repartition(sf.col("pickup_borough"))

        writeTemporalDemand(m2Df, input.outputDir, tag)

        return m2Df
    }
}
