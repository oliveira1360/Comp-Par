package compar.profiler

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import org.apache.spark.sql.SparkSession
import java.io.File
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID
import kotlin.math.abs
import kotlin.math.roundToLong

/**
 * Profiling utility for Spark pipelines in Kotlin.
 *
 * Collects stage, shuffle, memory, spill, GC, and task skew metrics via Spark's
 * status tracker and Web UI REST API (`/api/v1/applications/<app-id>/stages`).
 */
class SparkProfiler(spark: SparkSession) {
    private val sc = spark.sparkContext()
    private val appId: String = sc.applicationId()
    private val httpClient: HttpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .build()
    private val mapper = ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
    private val uiBaseUrl: String = detectUiBaseUrl()

    val runs: MutableMap<String, RunMetrics> = linkedMapOf()

    private fun detectUiBaseUrl(): String {
        val rawUrl = sc.uiWebUrl().takeIf { it.isDefined }?.get()
        if (rawUrl != null && fetchJson("$rawUrl/api/v1/applications", timeoutMs = 800) != null) {
            return rawUrl
        }
        val port = rawUrl?.substringAfterLast(":", "4040") ?: "4040"
        return "http://localhost:$port"
    }

    private fun fetchJson(url: String, timeoutMs: Long = 5000): JsonNode? = try {
        val request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofMillis(timeoutMs))
            .GET()
            .build()
        val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
        if (response.statusCode() in 200..299) mapper.readTree(response.body()) else null
    } catch (_: Exception) {
        null
    }

    /**
     * Wraps the code block to measure, assigning it a JobGroup and collecting metrics at the end.
     */
    fun <T> profile(name: String, block: () -> T): T {
        val groupId = "$name-${UUID.randomUUID().toString().replace("-", "").take(8)}"
        sc.setJobGroup(groupId, name, false)
        val wallStartNs = System.nanoTime()
        try {
            return block()
        } finally {
            val wallTimeS = (System.nanoTime() - wallStartNs) / 1_000_000_000.0
            sc.clearJobGroup()
            awaitJobGroup(groupId)

            val metrics = collect(groupId, wallTimeS)
            runs[name] = metrics
            println(
                String.format(
                    "[%s] Wall time: %.2fs | Stages: %d | Tasks: %d | Shuffle Read: %s | Shuffle Write: %s | Max Task: %dms | Skew: %.1fx",
                    name, wallTimeS, metrics.numStages, metrics.numTasks,
                    fmtBytes(metrics.shuffleReadBytes), fmtBytes(metrics.shuffleWriteBytes),
                    metrics.maxTaskDurationMs, metrics.skewRatio
                )
            )
        }
    }

    private fun awaitJobGroup(groupId: String) {
        try {
            val status = sc.statusTracker()
            val deadline = System.currentTimeMillis() + 1000
            while (System.currentTimeMillis() < deadline) {
                val jobIds = status.getJobIdsForGroup(groupId)
                if (jobIds.isNotEmpty() && jobIds.all { jid ->
                        val info = status.getJobInfo(jid)
                        info.isDefined && info.get().status().name != "RUNNING"
                    }) {
                    break
                }
                Thread.sleep(30)
            }
            // Brief grace period for the REST API stage metrics to publish
            Thread.sleep(100)
        } catch (_: Exception) {
        }
    }

    private fun collect(groupId: String, wallTimeS: Double): RunMetrics {
        val roundedWall = (wallTimeS * 100.0).roundToLong() / 100.0
        val status = sc.statusTracker()
        val jobIds = status.getJobIdsForGroup(groupId)

        val stageIds = buildSet {
            for (jid in jobIds) {
                val infoOpt = status.getJobInfo(jid)
                if (infoOpt.isDefined) addAll(infoOpt.get().stageIds().toList())
            }
        }

        val allStages = fetchJson("$uiBaseUrl/api/v1/applications/$appId/stages", timeoutMs = 10_000)
            ?.takeIf { it.isArray }
            ?: return RunMetrics(wallTimeS = roundedWall)

        // Select the latest attempt per stageId to avoid counting retries twice
        val wanted = allStages
            .filter { it.path("stageId").asInt(-1) in stageIds }
            .groupBy { it.path("stageId").asInt() }
            .values
            .map { attempts -> attempts.maxBy { it.path("attemptId").asInt(0) } }
            .sortedBy { it.path("stageId").asInt() }

        val stageSummaries = wanted.map { st -> buildStageSummary(st) }
        val dominantStage = stageSummaries.maxByOrNull { it.maxTaskDurationMs }

        fun sumField(field: String): Long = wanted.sumOf { it.path(field).asLong(0L) }

        return RunMetrics(
            wallTimeS = roundedWall,
            numJobs = jobIds.size,
            numStages = wanted.size,
            numTasks = sumField("numTasks"),
            executorRunTimeMs = sumField("executorRunTime"),
            executorCpuTimeMs = sumField("executorCpuTime") / 1_000_000L,
            jvmGcTimeMs = sumField("jvmGcTime"),
            peakExecutionMemoryBytes = wanted.maxOfOrNull { it.path("peakExecutionMemory").asLong(0L) } ?: 0L,
            inputBytes = sumField("inputBytes"),
            outputBytes = sumField("outputBytes"),
            shuffleReadBytes = sumField("shuffleReadBytes"),
            shuffleWriteBytes = sumField("shuffleWriteBytes"),
            shuffleReadRecords = sumField("shuffleReadRecords"),
            shuffleWriteRecords = sumField("shuffleWriteRecords"),
            memorySpilledBytes = sumField("memoryBytesSpilled"),
            diskSpilledBytes = sumField("diskBytesSpilled"),
            maxTaskDurationMs = dominantStage?.maxTaskDurationMs ?: 0L,
            medianTaskDurationMs = dominantStage?.medianTaskDurationMs ?: 0L,
            stages = stageSummaries
        )
    }

    private fun buildStageSummary(st: JsonNode): StageSummary {
        val sId = st.path("stageId").asInt(-1)
        val attemptId = st.path("attemptId").asInt(0)
        val numTasks = st.path("numTasks").asInt(0)

        var medianTaskMs = 0L
        var maxTaskMs = 0L
        if (numTasks > 0) {
            val runTimes = fetchJson(
                "$uiBaseUrl/api/v1/applications/$appId/stages/$sId/$attemptId/taskSummary?quantiles=0.5,1.0",
                timeoutMs = 2000
            )?.path("executorRunTime")
            if (runTimes != null && runTimes.isArray && runTimes.size() >= 2) {
                medianTaskMs = runTimes.get(0).asLong(0L)
                maxTaskMs = runTimes.get(1).asLong(0L)
            }
        }

        val displayName = st.path("description").asText(null)?.takeIf { it.isNotBlank() }
            ?: st.path("name").asText("unknown")

        return StageSummary(
            stageId = sId,
            name = displayName,
            numTasks = numTasks,
            executorRunTimeMs = st.path("executorRunTime").asLong(0L),
            executorCpuTimeMs = st.path("executorCpuTime").asLong(0L) / 1_000_000L,
            jvmGcTimeMs = st.path("jvmGcTime").asLong(0L),
            shuffleReadBytes = st.path("shuffleReadBytes").asLong(0L),
            shuffleWriteBytes = st.path("shuffleWriteBytes").asLong(0L),
            memorySpilledBytes = st.path("memoryBytesSpilled").asLong(0L),
            diskSpilledBytes = st.path("diskBytesSpilled").asLong(0L),
            peakExecutionMemoryBytes = st.path("peakExecutionMemory").asLong(0L),
            maxTaskDurationMs = maxTaskMs,
            medianTaskDurationMs = medianTaskMs
        )
    }

    /**
     * Prints the full profiling summary (metrics table, stage breakdown for skewed operations, and optional JSON export).
     */
    fun printSummary(exportPath: String? = null) {
        println("\n================================================================================")
        println("  Performance Engineering Report (SparkProfiler)")
        println("================================================================================")
        println(report())

        val skewedRuns = runs.filterValues { it.hasSignificantSkew }
        if (skewedRuns.isNotEmpty()) {
            println("\n[Warning] [SparkProfiler] Significant task skew detected (Task Skew >= ${StageSummary.SKEW_WARNING_THRESHOLD}x):")
            for (name in skewedRuns.keys) {
                println("\n--- Stage Breakdown for $name ---")
                println(stageBreakdown(name))
            }
        }

        if (!exportPath.isNullOrBlank()) {
            try {
                exportJson(exportPath)
            } catch (e: Exception) {
                System.err.println("Error exporting JSON metrics: ${e.message}")
            }
        }
    }

    /**
     * Generates a formatted table report for the console.
     */
    fun report(names: List<String> = runs.keys.toList()): String {
        if (names.isEmpty()) return "(No executions recorded in SparkProfiler)"

        val headers = listOf(
            "run", "wall_time_s", "num_stages", "num_tasks",
            "exec_run_ms", "exec_cpu_ms", "jvm_gc_ms",
            "input", "output", "shuffle_read", "shuffle_write",
            "mem_spill", "disk_spill", "peak_mem", "task_skew"
        )

        val rows = names.mapNotNull { name ->
            val m = runs[name] ?: return@mapNotNull null
            listOf(
                name,
                String.format("%.2f", m.wallTimeS),
                m.numStages.toString(),
                m.numTasks.toString(),
                m.executorRunTimeMs.toString(),
                m.executorCpuTimeMs.toString(),
                m.jvmGcTimeMs.toString(),
                fmtBytes(m.inputBytes),
                fmtBytes(m.outputBytes),
                fmtBytes(m.shuffleReadBytes),
                fmtBytes(m.shuffleWriteBytes),
                fmtBytes(m.memorySpilledBytes),
                fmtBytes(m.diskSpilledBytes),
                fmtBytes(m.peakExecutionMemoryBytes),
                m.formatSkew()
            )
        }

        return formatTable(headers, rows)
    }

    /**
     * Details the stages of a specific operation (useful for diagnosing skew, spills, and shuffle).
     */
    fun stageBreakdown(runName: String): String {
        val m = runs[runName] ?: return "Execution '$runName' not found."
        if (m.stages.isEmpty()) return "No stage details available for '$runName'."

        val headers = listOf(
            "stage_id", "name", "num_tasks", "run_time_ms", "cpu_time_ms",
            "gc_time_ms", "peak_mem", "shuffle_read", "shuffle_write",
            "med_task_ms", "max_task_ms", "skew"
        )
        val rows = m.stages.map { st ->
            listOf(
                st.stageId.toString(),
                st.name.take(30),
                st.numTasks.toString(),
                st.executorRunTimeMs.toString(),
                st.executorCpuTimeMs.toString(),
                st.jvmGcTimeMs.toString(),
                fmtBytes(st.peakExecutionMemoryBytes),
                fmtBytes(st.shuffleReadBytes),
                fmtBytes(st.shuffleWriteBytes),
                st.medianTaskDurationMs.toString(),
                st.maxTaskDurationMs.toString(),
                st.formatSkew()
            )
        }
        return formatTable(headers, rows)
    }

    /**
     * Exports the execution and stage data to a structured JSON file.
     */
    fun exportJson(filePath: String) {
        val targetPath = if (File("/output").isDirectory && (filePath.startsWith("output/") || filePath == "output")) {
            "/output" + filePath.removePrefix("output")
        } else {
            filePath
        }
        val file = File(targetPath)
        file.parentFile?.mkdirs()
        mapper.writeValue(file, runs)
        println("[SparkProfiler] Detailed report exported to: ${file.absolutePath}")
    }

    companion object {
        fun fmtBytes(bytes: Long): String {
            var n = bytes.toDouble()
            for (unit in listOf("B", "KB", "MB", "GB", "TB")) {
                if (abs(n) < 1024.0) return String.format("%.1f%s", n, unit)
                n /= 1024.0
            }
            return String.format("%.1fPB", n)
        }

        private fun formatTable(headers: List<String>, rows: List<List<String>>): String {
            val widths = headers.indices.map { col ->
                maxOf(headers[col].length, rows.maxOfOrNull { it[col].length } ?: 0)
            }
            val topBottomBorder = widths.joinToString("-+-", "+-", "-+") { "-".repeat(it + 2) }
            val headerBorder = widths.joinToString("=+=", "+=", "=+"){ "=".repeat(it + 2) }
            fun formatRow(cells: List<String>): String =
                cells.mapIndexed { i, cell -> " " + cell.padEnd(widths[i]) + " " }.joinToString(" | ", "| ", " |")

            val sb = StringBuilder()
            sb.appendLine(topBottomBorder)
            sb.appendLine(formatRow(headers))
            sb.appendLine(headerBorder)
            for (row in rows) {
                sb.appendLine(formatRow(row))
            }
            sb.appendLine(topBottomBorder)
            return sb.toString().trimEnd()
        }
    }
}
