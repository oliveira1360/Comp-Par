package compar.ops.m2

import compar.ops.Analysis
import compar.ops.AnalysisInput
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.io.File
import java.net.URI

/**
 * M2 v7 — direct driver CSV parsing to eliminate Spark lookup stages.
 * Bottleneck (v6): calling collectAsList() on input.zones launches 2-3 small Spark stages on the
 * cluster just to read a 12 KB (265 rows) CSV file, adding scheduling overhead and latency.
 * Change: parse the zone lookup CSV directly on the driver via standard I/O (with fallback to Spark),
 * completely eliminating the lookup stages, combined with O(1) array lookup and coalesce(1) write.
 */
class M2V7 : Analysis {

    override fun run(input: AnalysisInput): Dataset<Row> {
        println("\n--- [$tag] Temporal Demand Analysis ---")

        // Step 1: Parse the 12 KB zone lookup CSV directly on the driver without launching Spark jobs
        val (boroughs, boroughByZoneId) = buildZoneLookupArrayDirect(input.zones)
        val boroughDayHourKey = packBoroughDayHour(lookupBoroughId(boroughByZoneId))

        // Step 2: Single aggregation by the 11-bit key; divide mean duration by 60.0 only on the <= 1,176 groups
        val aggregated = aggregateBoroughDayHour(input.trips, boroughDayHourKey)

        // Step 3: Decode the 11-bit key back into (pickup_borough, day_of_week, hour_of_day) and coalesce(1)
        val m2Df = decodeBoroughDayHour(aggregated, boroughs)
            .coalesce(1)

        writeTemporalDemand(m2Df, input.outputDir)

        return m2Df
    }
}

/** Parses zone lookup CSV directly on the driver to avoid Spark job launch overhead for 12 KB. */
internal fun buildZoneLookupArrayDirect(zones: Dataset<Row>): ZoneLookup {
    val localFilePath = zones.inputFiles().firstOrNull()?.let { uriStr ->
        try {
            val uri = URI(uriStr)
            if (uri.scheme == null || uri.scheme == "file") uri.path else null
        } catch (_: Exception) {
            null
        }
    }

    val rows: List<Pair<Int, String>> = if (localFilePath != null && File(localFilePath).exists()) {
        File(localFilePath).bufferedReader().useLines { lines ->
            lines.drop(1).mapNotNull { line ->
                val tokens = line.split(",")
                if (tokens.size >= 3) {
                    val id = tokens[0].trim().toIntOrNull()
                    val boro = tokens[1].trim().trim('"')
                    if (id != null && boro.isNotEmpty()) id to boro else null
                } else null
            }.toList()
        }
    } else {
        // Fallback to Spark collection if local file path is not directly accessible
        val zoneRows = zones.select(COL_LOCATION_ID, COL_BOROUGH).collectAsList()
        zoneRows.mapNotNull {
            if (!it.isNullAt(ZONE_LOCATION_ID_IDX) && !it.isNullAt(ZONE_BOROUGH_IDX)) {
                it.getInt(ZONE_LOCATION_ID_IDX) to it.getString(ZONE_BOROUGH_IDX)
            } else null
        }
    }

    return createZoneLookup(rows)
}
