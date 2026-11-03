package compar.bench

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import java.io.File
import java.util.Locale
import kotlin.math.abs
import kotlin.math.max

/**
 * Aggregates the local benchmark matrix (scripts/local_bench/matrix.sh): median metrics per (operation, version)
 * and result checks of every version against v0. Writes summary.csv and summary.md into the bench directory.
 * Run with scripts/local_bench/analyze.sh [bench_dir] (default: build/local-bench).
 */

private val OPS = linkedMapOf(
    "M1" to "M1_ingestion_and_cleaning",
    "M2" to "M2_temporal_demand",
    "M3" to "M3_zone_statistics",
    "O1" to "O1_fare_anomaly_detection",
    "O4" to "O4_trip_type_classification"
)

private val FIELDS = listOf(
    "wallTimeS", "numJobs", "numStages", "numTasks", "executorRunTimeMs", "executorCpuTimeMs", "jvmGcTimeMs",
    "inputBytes", "outputBytes", "shuffleReadBytes", "shuffleWriteBytes", "memorySpilledBytes",
    "diskSpilledBytes", "peakExecutionMemoryBytes", "maxTaskDurationMs", "medianTaskDurationMs"
)

private val RUN_FILE = Regex("""(.+)_(v\d|best)_r(\d+)\.json""")

/** Medians of one (group, op, version); group is the config name (M1..O4, or ALL for the full pipeline). */
private class Summary(
    val group: String, val op: String, val version: String, val n: Int,
    val wallMin: Double, val wallMax: Double, val median: Map<String, Double>
) {
    operator fun get(field: String): Double = median[field] ?: 0.0
}

private fun median(values: List<Double>): Double {
    val s = values.sorted()
    return if (s.size % 2 == 1) s[s.size / 2] else (s[s.size / 2 - 1] + s[s.size / 2]) / 2
}

private fun summarise(dir: File): List<Summary> {
    val mapper = ObjectMapper()
    val runs = (File(dir, "metrics").listFiles() ?: emptyArray())
        .mapNotNull { f -> RUN_FILE.matchEntire(f.name)?.let { (it.groupValues[1] to it.groupValues[2]) to mapper.readTree(f) } }
        .groupBy({ it.first }, { it.second })
        .toSortedMap(compareBy<Pair<String, String>> { it.first }.thenBy { it.second })

    val rows = mutableListOf<Summary>()
    for ((key, items) in runs) {
        val (group, version) = key
        val ops = if (group.startsWith("ALL")) OPS.keys.toList() else listOf(group)
        for (op in ops) {
            val vals: List<JsonNode> = items.mapNotNull { it.get(OPS[op]) }
            if (vals.isEmpty()) continue
            val walls = vals.map { it.path("wallTimeS").asDouble() }
            rows += Summary(group, op, version, vals.size, walls.min(), walls.max(),
                FIELDS.associateWith { f -> median(vals.map { it.path(f).asDouble() }) })
        }
        if (group.startsWith("ALL")) {
            val totals = items.map { d -> OPS.values.sumOf { d.path(it).path("wallTimeS").asDouble() } }
            rows += Summary(group, "TOTAL", version, totals.size, totals.min(), totals.max(), mapOf("wallTimeS" to median(totals)))
        }
    }
    return rows
}

private fun mb(bytes: Double) = "%.1f".format(bytes / 1e6)
private fun s(ms: Double) = "%.1f".format(ms / 1000)

private fun writeCsv(dir: File, rows: List<Summary>) {
    File(dir, "summary.csv").printWriter().use { out ->
        out.println((listOf("group", "op", "version", "n", "wallTimeS", "wall_min", "wall_max") + FIELDS.drop(1)).joinToString(","))
        for (r in rows) {
            val values = listOf(r.group, r.op, r.version, r.n, r["wallTimeS"], r.wallMin, r.wallMax) +
                FIELDS.drop(1).map { r.median[it]?.toString() ?: "" }
            out.println(values.joinToString(","))
        }
    }
}

/** One Markdown table per operation, with the speedup against v0 and against the previous version. */
private fun writeMarkdown(dir: File, rows: List<Summary>) {
    File(dir, "summary.md").printWriter(Charsets.UTF_8).use { md ->
        for (op in OPS.keys) {
            val own = rows.filter { it.group == op }
            if (own.isEmpty()) continue
            md.println("\n### $op\n")
            md.println("| versão | wall (s) mediana [min–max] | vs v0 | vs anterior | stages | tasks | CPU (s) | GC (s) " +
                "| input (MB) | shuffle R/W (MB) | spill (MB) | peak exec. mem (MB) |")
            md.println("|" + "---|".repeat(12))
            val base = own.first()["wallTimeS"]
            var prev: Double? = null
            for (r in own) {
                val wall = r["wallTimeS"]
                val vsPrev = prev?.let { "%.2f×".format(it / wall) } ?: "—"
                md.println("| ${r.version} | ${"%.2f [%.2f–%.2f]".format(wall, r.wallMin, r.wallMax)} | ${"%.2f×".format(base / wall)} " +
                    "| $vsPrev | ${r["numStages"].toLong()} | ${r["numTasks"].toLong()} | ${s(r["executorCpuTimeMs"])} " +
                    "| ${s(r["jvmGcTimeMs"])} | ${mb(r["inputBytes"])} | ${mb(r["shuffleReadBytes"])} / ${mb(r["shuffleWriteBytes"])} " +
                    "| ${mb(r["memorySpilledBytes"] + r["diskSpilledBytes"])} | ${mb(r["peakExecutionMemoryBytes"])} |")
                prev = wall
            }
        }
    }
}

private fun printTable(rows: List<Summary>) {
    println("%-11s%-5s%-6s%2s%8s%7s%7s%7s%7s%7s%6s%8s%9s%9s%9s%8s".format(
        "group", "op", "ver", "n", "wall_s", "min", "max", "stages", "tasks", "cpu_s", "gc_s",
        "in_MB", "shufR_MB", "shufW_MB", "spill_MB", "peak_MB"))
    for (r in rows) {
        val head = "%-11s%-5s%-6s%2d%8.2f%7.2f%7.2f".format(r.group,
            if (r.group.startsWith("ALL") && r.op != "TOTAL") r.op + "*" else r.op, r.version, r.n, r["wallTimeS"], r.wallMin, r.wallMax)
        if (r.op == "TOTAL") { println(head); continue }
        println(head + "%7d%7d%7s%6s%8s%9s%9s%9s%8s".format(
            r["numStages"].toLong(), r["numTasks"].toLong(), s(r["executorCpuTimeMs"]), s(r["jvmGcTimeMs"]),
            mb(r["inputBytes"]), mb(r["shuffleReadBytes"]), mb(r["shuffleWriteBytes"]),
            mb(r["memorySpilledBytes"] + r["diskSpilledBytes"]), mb(r["peakExecutionMemoryBytes"])))
    }
    println("(* = inside a full pipeline run ALL*_<version>)")
}

// ---- result checks (round 1 of every config) ----

private fun log(dir: File, name: String): String =
    File(dir, "logs/$name.log").takeIf { it.exists() }?.readText(Charsets.UTF_8) ?: ""

private val M1_COUNTER = Regex("""] (Total records read|Valid records|Total removed records): (.+)""")
private val M3_FLAGS = Regex("""volume deviation > 1\.5 std: (\S+)""")

/** Reads a Parquet output (partition columns included) keyed by the given columns; null when missing. */
private fun table(spark: SparkSession, path: File, keys: List<String>): Map<List<String>, Row>? {
    if (!path.exists()) return null
    return spark.read().parquet(path.path).collectAsList().associateBy { row -> keys.map { row.getAs<Any?>(it).toString() } }
}

private fun compareTables(a: Map<List<String>, Row>?, b: Map<List<String>, Row>?): String {
    if (a == null || b == null) return "missing output"
    if (a.keys != b.keys) return "different keys (${a.size} vs ${b.size})"
    val worst = mutableMapOf<String, Double>()
    val differing = mutableMapOf<String, Int>()
    for ((key, ra) in a) {
        val rb = b.getValue(key)
        for (col in ra.schema().fieldNames()) {
            val x = ra.getAs<Any?>(col)
            val y = rb.getAs<Any?>(col)
            if (x is Double && y is Double) {
                val d = abs(x - y) / max(abs(x), 1e-300)
                worst[col] = max(worst[col] ?: 0.0, d)
                if (d > 1e-9) differing.merge(col, 1, Int::plus)
            } else if (x.toString() != y.toString()) {
                return "different value in $col: $x vs $y"
            }
        }
    }
    if (differing.isEmpty()) return "same ${a.size} rows (worst relative difference %.1e)".format(worst.values.maxOrNull() ?: 0.0)
    return "same ${a.size} keys; differing rows per column: " +
        differing.entries.joinToString { (c, n) -> "$c=$n (worst %.1e)".format(worst.getValue(c)) }
}

private fun checkResults(dir: File) {
    println("\n== result checks against v0 (round 1)")
    val m1 = { v: String -> M1_COUNTER.findAll(log(dir, "M1_${v}_r1")).map { "${it.groupValues[1]}: ${it.groupValues[2]}" }.toList() }
    for (v in listOf("v1", "v2", "v3")) println("M1 $v ${m1(v)} | v0: ${m1("v0")}")

    val builder = SparkSession.builder().master("local[2]").appName("bench-analyze").config("spark.ui.enabled", "false")
    if (System.getProperty("os.name").startsWith("Windows")) {
        builder.config("spark.hadoop.fs.file.impl", NoNativeLocalFs::class.java.name)
            .config("spark.hadoop.fs.file.impl.disable.cache", "true")
    }
    val spark = builder.getOrCreate()
    spark.sparkContext().setLogLevel("ERROR")

    val out = File(dir, "out")
    val m2Keys = listOf("pickup_borough", "day_of_week", "hour_of_day")
    val m2Base = table(spark, File(out, "M2_v0_r1/m2_temporal_demand"), m2Keys)
    for (v in (1..7).map { "v$it" }) {
        println("M2 $v " + compareTables(m2Base, table(spark, File(out, "M2_${v}_r1/m2_temporal_demand"), m2Keys)))
    }

    val flags = listOf("v0", "v1", "v2", "v3").associateWith { v -> M3_FLAGS.findAll(log(dir, "M3_${v}_r1")).map { it.groupValues[1] }.toList() }
    println("M3 flagged zone-months per version: $flags")

    val o1Keys = listOf("pickup_borough", "payment_type", "day_type")
    val o1Base = table(spark, File(out, "O1_v0_r1/o1_fare_anomalies"), o1Keys)
    for (v in (1..7).map { "v$it" }) {
        println("O1 $v " + compareTables(o1Base, table(spark, File(out, "O1_${v}_r1/o1_fare_anomalies"), o1Keys)))
    }
    spark.stop()

    val errors = Regex("Exception in thread|Caused by")
    (File(dir, "logs").listFiles() ?: emptyArray()).sortedBy { it.name }
        .filter { errors.containsMatchIn(it.readText(Charsets.UTF_8)) }
        .forEach { println("ERROR in ${it.nameWithoutExtension}") }
}

fun main(args: Array<String>) {
    Locale.setDefault(Locale.ROOT) // dot decimals in the CSV and the tables
    val dir = File(args.getOrElse(0) { "build/local-bench" }).absoluteFile
    val rows = summarise(dir)
    writeCsv(dir, rows)
    writeMarkdown(dir, rows)
    printTable(rows)
    checkResults(dir)
    println("\nwritten: ${File(dir, "summary.csv")}, ${File(dir, "summary.md")}")
}
