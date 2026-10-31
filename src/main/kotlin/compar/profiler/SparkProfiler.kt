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
 * Enhanced profiling utility for Spark pipelines in Kotlin.
 *
 * Uses Spark's native status tracker and the Web UI REST API
 * (`http://<driver-host>:<ui-port>/api/v1/applications/<app-id>/stages`)
 * to collect key performance metrics:
 * - Execution and CPU times
 * - Garbage Collection overhead (JVM GC Time)
 * - Peak Execution Memory
 * - Shuffle volumes and counts (Read / Write Bytes and Records)
 * - Memory and disk spills
 * - Per-stage details and potential skew
 * - JSON and Markdown export
 */
class SparkProfiler(
    spark: SparkSession,
    uiBaseUrlOverride: String? = null
) {
    private val sc = spark.sparkContext()
    private val appId: String = sc.applicationId()
    private val uiBaseUrl: String = uiBaseUrlOverride ?: detectUiBaseUrl()
    private val httpClient: HttpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .build()
    private val mapper = ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)

    val runs: MutableMap<String, RunMetrics> = linkedMapOf()

    private fun detectUiBaseUrl(): String {
        val urlOpt = sc.uiWebUrl()
        val rawUrl = if (urlOpt.isDefined) urlOpt.get() else null
        val port = rawUrl?.substringAfterLast(":", "4040") ?: "4040"
        
        // Try the original URL first; if it fails, try localhost
        if (rawUrl != null && isReachable(rawUrl)) {
            return rawUrl
        }
        val localhostUrl = "http://localhost:$port"
        if (isReachable(localhostUrl)) {
            return localhostUrl
        }
        val loopbackUrl = "http://127.0.0.1:$port"
        if (isReachable(loopbackUrl)) {
            return loopbackUrl
        }
        return localhostUrl
    }

    private fun isReachable(urlStr: String): Boolean {
        return try {
            val req = HttpRequest.newBuilder()
                .uri(URI.create("$urlStr/api/v1/applications"))
                .timeout(Duration.ofMillis(800))
                .GET()
                .build()
            val resp = httpClient.send(req, HttpResponse.BodyHandlers.discarding())
            resp.statusCode() in 200..299
        } catch (_: Exception) {
            false
        }
    }

    private fun getJson(path: String): JsonNode {
        val request = HttpRequest.newBuilder()
            .uri(URI.create("$uiBaseUrl$path"))
            .timeout(Duration.ofSeconds(10))
            .GET()
            .build()
        val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
        if (response.statusCode() !in 200..299) {
            throw RuntimeException("HTTP ${response.statusCode()} accessing $uiBaseUrl$path")
        }
        return mapper.readTree(response.body())
    }

    private fun getJsonOrNull(path: String): JsonNode? {
        return try {
            val request = HttpRequest.newBuilder()
                .uri(URI.create("$uiBaseUrl$path"))
                .timeout(Duration.ofSeconds(2))
                .GET()
                .build()
            val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
            if (response.statusCode() in 200..299) {
                mapper.readTree(response.body())
            } else {
                null
            }
        } catch (_: Exception) {
            null
        }
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
            try {
                sc.clearJobGroup()
                // Wait briefly for LiveListenerBus to finish processing events for this job group
                val status = sc.statusTracker()
                val deadline = System.currentTimeMillis() + 1000
                while (System.currentTimeMillis() < deadline) {
                    val jobIds = status.getJobIdsForGroup(groupId)
                    if (jobIds.isNotEmpty()) {
                        val allDone = jobIds.all { jid ->
                            val info = status.getJobInfo(jid)
                            info.isDefined && info.get().status().name != "RUNNING"
                        }
                        if (allDone) break
                    }
                    Thread.sleep(30)
                }
                // Short grace period so the REST API's internal stage metrics are fully published
                Thread.sleep(100)
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
            } catch (e: Exception) {
                // Graceful fallback with basic metrics if the UI REST API fails
                val fallbackMetrics = RunMetrics(
                    wallTimeS = (wallTimeS * 100.0).roundToLong() / 100.0,
                    numJobs = 0,
                    numStages = 0,
                    numTasks = 0,
                    executorRunTimeMs = 0,
                    executorCpuTimeMs = 0,
                    jvmGcTimeMs = 0,
                    peakExecutionMemoryBytes = 0,
                    inputBytes = 0,
                    outputBytes = 0,
                    shuffleReadBytes = 0,
                    shuffleWriteBytes = 0,
                    shuffleReadRecords = 0,
                    shuffleWriteRecords = 0,
                    memorySpilledBytes = 0,
                    diskSpilledBytes = 0
                )
                runs[name] = fallbackMetrics
                println(
                    String.format(
                        "[%s] Wall time: %.2fs (REST API unavailable: %s)",
                        name, wallTimeS, e.message
                    )
                )
            }
        }
    }

    private fun collect(groupId: String, wallTimeS: Double): RunMetrics {
        val status = sc.statusTracker()
        val jobIds = status.getJobIdsForGroup(groupId)

        val stageIds = mutableSetOf<Int>()
        for (jid in jobIds) {
            val infoOpt = status.getJobInfo(jid)
            if (infoOpt.isDefined) {
                stageIds.addAll(infoOpt.get().stageIds().toList())
            }
        }

        val allStages = getJson("/api/v1/applications/$appId/stages")
        // Group by stageId and select the latest attempt to avoid duplicate metrics from retries
        val stageMap = mutableMapOf<Int, JsonNode>()
        if (allStages.isArray) {
            for (stage in allStages) {
                val sId = stage.path("stageId").asInt(-1)
                if (sId in stageIds) {
                    val attemptId = stage.path("attemptId").asInt(0)
                    val existing = stageMap[sId]
                    if (existing == null || attemptId > existing.path("attemptId").asInt(0)) {
                        stageMap[sId] = stage
                    }
                }
            }
        }
        val wanted = stageMap.values.sortedBy { it.path("stageId").asInt() }

        fun sumField(field: String): Long =
            wanted.sumOf { it.path(field).asLong(0L) }

        val roundedWall = (wallTimeS * 100.0).roundToLong() / 100.0

        val stageSummaries = wanted.map { st ->
            val sId = st.path("stageId").asInt(-1)
            val attemptId = st.path("attemptId").asInt(0)
            val numTasks = st.path("numTasks").asInt(0)

            var stageMedianTaskMs = 0L
            var stageMaxTaskMs = 0L

            if (numTasks > 0) {
                val summaryNode = getJsonOrNull("/api/v1/applications/$appId/stages/$sId/$attemptId/taskSummary?quantiles=0.5,1.0")
                if (summaryNode != null && summaryNode.has("executorRunTime")) {
                    val runTimes = summaryNode.path("executorRunTime")
                    if (runTimes.isArray && runTimes.size() >= 2) {
                        stageMedianTaskMs = runTimes.get(0).asLong(0L)
                        stageMaxTaskMs = runTimes.get(1).asLong(0L)
                    }
                }
            }

            val displayName = st.path("description").asText(null)
                ?.takeIf { it.isNotBlank() }
                ?: st.path("name").asText("unknown")

            StageSummary(
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
                maxTaskDurationMs = stageMaxTaskMs,
                medianTaskDurationMs = stageMedianTaskMs
            )
        }

        val overallMaxTask = stageSummaries.maxOfOrNull { it.maxTaskDurationMs } ?: 0L
        val dominantStage = stageSummaries.maxByOrNull { it.maxTaskDurationMs }
        val overallMedianTask = dominantStage?.medianTaskDurationMs ?: 0L
        val peakMemory = wanted.maxOfOrNull { it.path("peakExecutionMemory").asLong(0L) } ?: 0L

        return RunMetrics(
            wallTimeS = roundedWall,
            numJobs = jobIds.size,
            numStages = wanted.size,
            numTasks = sumField("numTasks"),
            executorRunTimeMs = sumField("executorRunTime"),
            // executorCpuTime comes in nanoseconds from the REST API; convert to ms
            executorCpuTimeMs = sumField("executorCpuTime") / 1_000_000L,
            jvmGcTimeMs = sumField("jvmGcTime"),
            peakExecutionMemoryBytes = peakMemory,
            inputBytes = sumField("inputBytes"),
            outputBytes = sumField("outputBytes"),
            shuffleReadBytes = sumField("shuffleReadBytes"),
            shuffleWriteBytes = sumField("shuffleWriteBytes"),
            shuffleReadRecords = sumField("shuffleReadRecords"),
            shuffleWriteRecords = sumField("shuffleWriteRecords"),
            memorySpilledBytes = sumField("memoryBytesSpilled"),
            diskSpilledBytes = sumField("diskBytesSpilled"),
            maxTaskDurationMs = overallMaxTask,
            medianTaskDurationMs = overallMedianTask,
            stages = stageSummaries
        )
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
            val skewDisplay = if (m.medianTaskDurationMs > 0) {
                val ratio = m.maxTaskDurationMs.toDouble() / m.medianTaskDurationMs
                String.format("%.1fx", ratio)
            } else "-"
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
                skewDisplay
            )
        }

        return formatTable(headers, rows)
    }

    /**
     * Generates a Markdown report for quick inclusion in the report or documentation.
     */
    fun reportMarkdown(names: List<String> = runs.keys.toList()): String {
        if (names.isEmpty()) return "*(No executions)*"

        val headers = listOf(
            "Run", "Wall Time (s)", "Stages", "Tasks", "Exec Time (ms)", "CPU Time (ms)",
            "GC Time (ms)", "Peak Mem", "Input", "Output", "Shuffle Read", "Shuffle Write",
            "Mem Spill", "Disk Spill", "Task Skew"
        )

        val rows = names.mapNotNull { name ->
            val m = runs[name] ?: return@mapNotNull null
            val skewDisplay = if (m.medianTaskDurationMs > 0) {
                val ratio = m.maxTaskDurationMs.toDouble() / m.medianTaskDurationMs
                String.format("%.1fx", ratio)
            } else "-"
            listOf(
                name,
                String.format("%.2f", m.wallTimeS),
                m.numStages.toString(),
                m.numTasks.toString(),
                m.executorRunTimeMs.toString(),
                m.executorCpuTimeMs.toString(),
                m.jvmGcTimeMs.toString(),
                fmtBytes(m.peakExecutionMemoryBytes),
                fmtBytes(m.inputBytes),
                fmtBytes(m.outputBytes),
                fmtBytes(m.shuffleReadBytes),
                fmtBytes(m.shuffleWriteBytes),
                fmtBytes(m.memorySpilledBytes),
                fmtBytes(m.diskSpilledBytes),
                skewDisplay
            )
        }

        val sb = StringBuilder()
        sb.appendLine("| " + headers.joinToString(" | ") + " |")
        sb.appendLine("| " + headers.joinToString(" | ") { ":---" } + " |")
        for (r in rows) {
            sb.appendLine("| " + r.joinToString(" | ") + " |")
        }
        return sb.toString().trimEnd()
    }

    /**
     * Details the stages of a specific operation (useful for diagnosing skew and shuffle).
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
            val skewStr = if (st.medianTaskDurationMs > 0) {
                val ratio = st.maxTaskDurationMs.toDouble() / st.medianTaskDurationMs
                val flag = if (ratio >= 3.0) " (!)" else ""
                String.format("%.1fx%s", ratio, flag)
            } else "-"
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
                skewStr
            )
        }
        return formatTable(headers, rows)
    }

    /**
     * Compares two executions (baseline vs optimized), showing the percentage change.
     */
    fun compare(before: String, after: String): String {
        val b = runs[before]?.toMap() ?: throw IllegalArgumentException("Run '$before' not found")
        val a = runs[after]?.toMap() ?: throw IllegalArgumentException("Run '$after' not found")

        val headers = listOf("metric", before, after, "change")
        val rows = b.keys.map { key ->
            val bv = b[key]!!
            val av = a[key]!!
            val change = if (bv.toDouble() != 0.0) {
                String.format("%+.1f%%", (av.toDouble() - bv.toDouble()) / bv.toDouble() * 100.0)
            } else {
                "-"
            }
            val bvDisp = if (key in BYTE_KEYS) fmtBytes(bv.toLong()) else bv.toString()
            val avDisp = if (key in BYTE_KEYS) fmtBytes(av.toLong()) else av.toString()
            listOf(key, bvDisp, avDisp, change)
        }

        return formatTable(headers, rows)
    }

    /**
     * Exports the execution and stage data to a structured JSON file.
     */
    fun exportJson(filePath: String) {
        val targetPath = resolveOutputPath(filePath)
        val file = File(targetPath)
        file.parentFile?.mkdirs()
        mapper.writeValue(file, runs)
        println("[SparkProfiler] Detailed report exported to: ${file.absolutePath}")
    }

    private fun resolveOutputPath(path: String): String {
        // If running inside Docker where /output is mounted, and path is relative "output/..."
        if (File("/output").isDirectory && (path.startsWith("output/") || path == "output")) {
            return "/output" + path.removePrefix("output")
        }
        return path
    }

    companion object {
        private val BYTE_KEYS = setOf(
            "input_bytes", "output_bytes", "shuffle_read_bytes",
            "shuffle_write_bytes", "memory_spilled_bytes", "disk_spilled_bytes",
            "peak_execution_memory_bytes"
        )

        fun fmtBytes(bytes: Long): String {
            var n = bytes.toDouble()
            for (unit in listOf("B", "KB", "MB", "GB", "TB")) {
                if (abs(n) < 1024.0) {
                    return String.format("%.1f%s", n, unit)
                }
                n /= 1024.0
            }
            return String.format("%.1fPB", n)
        }

        private fun formatTable(headers: List<String>, rows: List<List<String>>): String {
            val widths = headers.indices.map { col ->
                maxOf(headers[col].length, rows.maxOfOrNull { it[col].length } ?: 0)
            }
            val sb = StringBuilder()
            val headerLine = headers.mapIndexed { i, h -> h.padEnd(widths[i]) }.joinToString(" | ")
            val separatorLine = widths.joinToString("-+-") { "-".repeat(it) }
            sb.appendLine(headerLine)
            sb.appendLine(separatorLine)
            for (row in rows) {
                sb.appendLine(row.mapIndexed { i, cell -> cell.padEnd(widths[i]) }.joinToString(" | "))
            }
            return sb.toString().trimEnd()
        }
    }
}
