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
        println("\n--- [$tag] Temporal Demand Analysis ---")

        val m2Df = input.trips
            .select(*m2TripColumns)
            .join(sf.broadcast(pickupZones(input.zones)), COL_PU_LOCATION_ID)
            .let(::aggregateTemporalDemand)
            .orderBy(COL_PICKUP_BOROUGH, COL_DAY_OF_WEEK, COL_HOUR_OF_DAY)

        writeTemporalDemand(m2Df, input.outputDir)

        return m2Df
    }
}
