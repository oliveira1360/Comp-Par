package compar.ops.o1

import compar.ops.Analysis
import compar.ops.AnalysisInput
import compar.ops.common.pickupZones
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions as sf

/**
 * O1 v2 — Broadcast join on the zone lookup table.
 *
 * Bottleneck (v1):
 * - Joining trips with the zone lookup table without an explicit broadcast hint risks a full
 *   SortMergeJoin or distributed shuffle on millions of trip rows, even though taxi_zone_lookup.csv
 *   contains only 265 rows (~12 KB).
 *
 * Change:
 * - Wraps pickupZones(input.zones) in sf.broadcast(), forcing a BroadcastHashJoin.
 *   This distributes the tiny zone lookup table to all worker memory spaces, eliminating
 *   any shuffle or sort on the large trips dataset for the borough resolution step.
 * - Reuses selectTripMetrics() from v1, flagAnomaliesWithWindow() and aggregateAnomalyRates() from v0,
 *   and writes output to Parquet.
 */
class O1V2 : Analysis {

    override fun run(input: AnalysisInput): Dataset<Row> {
        println("\n--- [$tag] Fare Anomaly Detection ---")

        // 1. Column pruning (reused from v1)
        val dfWithMetrics = selectTripMetrics(input.trips)

        // 2 & 3. Compute bucket statistics (μ, σ) via Window and flag anomalies (reused from v0)
        val dfFlagged = flagAnomaliesWithWindow(dfWithMetrics)

        // 4. Broadcast join: distribute the 265-row zone table to avoid shuffling millions of trips
        val dfWithBorough = dfFlagged.join(sf.broadcast(pickupZones(input.zones)), COL_PU_LOCATION_ID)

        // 5. Compare weekday vs. weekend anomaly rates (reused from v0)
        val anomalyRatesDf = aggregateAnomalyRates(dfWithBorough)

        writeAnomalyRates(anomalyRatesDf, input.outputDir)

        return anomalyRatesDf
    }
}
