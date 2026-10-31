package compar.ops.m2

import compar.ops.Analysis
import compar.ops.AnalysisInput
import compar.ops.common.pickupZones
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions as sf

/**
 * M2 v1 — column pruning, broadcast join, and single action.
 * Bottleneck (v0): no explicit column projection or broadcast hint on the zone lookup join,
 * and calling show() before write() triggers two full evaluations of the query.
 * Change: select only the 5 required trip columns upfront, broadcast the small zone lookup table,
 * and remove the redundant show() action.
 */
class M2V1 : Analysis {

    override fun run(input: AnalysisInput): Dataset<Row> {
        val tag = javaClass.simpleName
        println("\n--- [$tag] Temporal Demand Analysis ---")

        val m2Df = input.trips
            .select(*m2TripColumns)
            .join(sf.broadcast(pickupZones(input.zones)), "PULocationID")
            .let(::aggregateTemporalDemand)
            .orderBy("pickup_borough", "day_of_week", "hour_of_day")

        writeTemporalDemand(m2Df, input.outputDir, tag)

        return m2Df
    }
}
