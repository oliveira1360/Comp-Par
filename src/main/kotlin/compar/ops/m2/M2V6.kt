package compar.ops.m2

import compar.ops.Analysis
import compar.ops.AnalysisInput
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

/**
 * M2 v6 — single-shuffle aggregation and coalesce(1) write.
 * Bottleneck (v5): calling repartition(col("pickup_borough")) after groupBy triggers a second
 * full shuffle stage just to redistribute at most 1,176 aggregated rows (~37 KB total).
 * Change: replace repartition(col("pickup_borough")) with coalesce(1) so that the tiny aggregated
 * result is written in a single partition without a second shuffle stage.
 */
class M2V6 : Analysis {

    override fun run(input: AnalysisInput): Dataset<Row> {
        println("\n--- [$tag] Temporal Demand Analysis ---")

        val (boroughs, boroughByZoneId) = buildZoneLookupArray(input.zones)
        val boroughDayHourKey = packBoroughDayHour(lookupBoroughId(boroughByZoneId))

        val aggregated = aggregateBoroughDayHour(input.trips, boroughDayHourKey)

        // Coalesce the <= 1,176 aggregated rows into 1 partition without triggering a second shuffle
        val m2Df = decodeBoroughDayHour(aggregated, boroughs)
            .coalesce(1)

        writeTemporalDemand(m2Df, input.outputDir)

        return m2Df
    }
}
