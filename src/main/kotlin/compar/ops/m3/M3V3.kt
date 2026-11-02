package compar.ops.m3

import compar.ops.Analysis
import compar.ops.AnalysisInput
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

/**
 * M3 v3 — windows on the driver.
 * Bottleneck (v2): after the aggregation only about 3 thousand rows remain, yet each of the window
 * specifications still needs its own shuffle and sort, with tasks scheduled for very little data.
 * Change: the aggregated rows are collected and the rank, the rolling share and the flag are computed
 * in plain loops on the driver. The only shuffle left is the one of the aggregation.
 */
class M3V3 : Analysis {

    private class ZoneMonth(
        val zoneId: Int, val borough: String?, val zone: String?, val yearMonth: String, val volume: Long,
        val meanFare: Double, val stddevFare: Double?, val meanDistance: Double, val stddevDistance: Double?
    ) {
        var rank = 0
        var share = 0.0
        var rollingShare = 0.0
        var rollingMean: Double? = null
        var rollingStddev: Double? = null
        var flagged = false
    }

    override fun run(input: AnalysisInput): Dataset<Row> {
        println("\n--- [$tag] Zone-Level Statistics ---")

        val zoneNames = pickupZoneNames(input.zones).collectAsList()
            .associate { it.getInt(0) to Pair(it.getString(1), it.getString(2)) }

        val rows = aggregateByZoneMonthKey(input.trips).collectAsList().mapNotNull { row ->
            val zoneId = row.getInt(0)
            // Same as the inner join of v0: zones missing from the table are dropped
            val names = zoneNames[zoneId] ?: return@mapNotNull null
            val key = row.getInt(1)
            ZoneMonth(
                zoneId, names.first, names.second, String.format("%04d-%02d", key / 100, key % 100), row.getLong(2),
                row.getDouble(3), if (row.isNullAt(4)) null else row.getDouble(4),
                row.getDouble(5), if (row.isNullAt(6)) null else row.getDouble(6)
            )
        }

        computeBoroughRanks(rows)
        computeRollingStats(rows)

        val sorted = rows.sortedWith(
            compareBy<ZoneMonth> { it.yearMonth }.thenBy(nullsFirst()) { it.borough }.thenBy { it.rank }
        )
        val m3Df = input.spark.createDataFrame(sorted.map { toRow(it) }, schema)

        m3Df.show(20, false)

        println(String.format("[$tag] Total zone-months with volume deviation > 1.5 std: %,d", rows.count { it.flagged }))

        return m3Df
    }

    private fun computeBoroughRanks(rows: List<ZoneMonth>) {
        for (group in rows.groupBy { Pair(it.borough, it.yearMonth) }.values) {
            val total = group.sumOf { it.volume }
            val byVolume = group.sortedByDescending { it.volume }
            for ((position, row) in byVolume.withIndex()) {
                row.rank = if (position > 0 && byVolume[position - 1].volume == row.volume) byVolume[position - 1].rank else position + 1
                row.share = if (total > 0) row.volume.toDouble() / total.toDouble() else 0.0
            }
        }
    }

    private fun computeRollingStats(rows: List<ZoneMonth>) {
        for (group in rows.groupBy { it.zoneId }.values) {
            val byMonth = group.sortedBy { it.yearMonth }
            for ((position, row) in byMonth.withIndex()) {
                val frame = byMonth.subList(maxOf(0, position - 2), position + 1)
                row.rollingShare = frame.sumOf { it.share } / frame.size

                val previous = byMonth.subList(maxOf(0, position - 3), position)
                val mean = if (previous.isEmpty()) null else previous.sumOf { it.volume.toDouble() } / previous.size
                val stddev = sampleStddev(previous)
                row.rollingMean = mean
                row.rollingStddev = stddev
                row.flagged = mean != null && stddev != null && Math.abs(row.volume - mean) > stddev * 1.5
            }
        }
    }

    /** Same incremental update that Spark uses, so the values match; null with fewer than 2 values. */
    private fun sampleStddev(frame: List<ZoneMonth>): Double? {
        if (frame.size < 2) return null
        var mean = 0.0
        var squares = 0.0
        var count = 0
        for (row in frame) {
            count++
            val delta = row.volume.toDouble() - mean
            val deltaN = delta / count
            mean += deltaN
            squares += delta * (delta - deltaN)
        }
        return Math.sqrt(squares / (count - 1))
    }

    private fun toRow(row: ZoneMonth): Row = RowFactory.create(
        row.zoneId, row.borough, row.zone, row.yearMonth, row.volume,
        row.meanFare, row.stddevFare, row.meanDistance, row.stddevDistance,
        row.rank, row.rollingShare, row.rollingMean, row.rollingStddev, row.flagged
    )

    private val schema = StructType(
        listOf(
            "PULocationID" to DataTypes.IntegerType,
            "pickup_borough" to DataTypes.StringType,
            "pickup_zone" to DataTypes.StringType,
            "year_month" to DataTypes.StringType,
            "monthly_trip_volume" to DataTypes.LongType,
            "mean_fare_amount" to DataTypes.DoubleType,
            "stddev_fare_amount" to DataTypes.DoubleType,
            "mean_trip_distance" to DataTypes.DoubleType,
            "stddev_trip_distance" to DataTypes.DoubleType,
            "borough_volume_rank" to DataTypes.IntegerType,
            "rolling_3m_trip_share" to DataTypes.DoubleType,
            "rolling_mean_volume" to DataTypes.DoubleType,
            "rolling_stddev_volume" to DataTypes.DoubleType,
            "volume_deviation_flag" to DataTypes.BooleanType
        ).map { (name, type) -> StructField.apply(name, type, true, Metadata.empty()) }.toTypedArray()
    )
}
