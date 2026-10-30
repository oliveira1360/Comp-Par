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
    private val spark: SparkSession,
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
            throw RuntimeException("HTTP ${response.statusCode()} ao aceder a $uiBaseUrl$path")
        }
        return mapper.readTree(response.body())
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
                // Short pause so the REST API's internal listeners are consistent
                Thread.sleep(80)
                val metrics = collect(groupId, wallTimeS)
                runs[name] = metrics
                println(
                    String.format(
                        "[%s] wall time: %.2fs (%d stages, %d tasks, shuffle read: %s, shuffle write: %s)",
                        name, wallTimeS, metrics.numStages, metrics.numTasks,
                        fmtBytes(metrics.shuffleReadBytes), fmtBytes(metrics.shuffleWriteBytes)
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
                        "[%s] wall time: %.2fs (REST API indisponível: %s)",
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
        val wanted = mutableListOf<JsonNode>()
        if (allStages.isArray) {
            for (stage in allStages) {
                if (stage.path("stageId").asInt(-1) in stageIds) {
                    wanted.add(stage)
                }
            }
        }

        fun sumField(field: String): Long =
            wanted.sumOf { it.path(field).asLong(0L) }

        val roundedWall = (wallTimeS * 100.0).roundToLong() / 100.0

        val stageSummaries = wanted.map { st ->
            StageSummary(
                stageId = st.path("stageId").asInt(-1),
                name = st.path("name").asText("unknown"),
                numTasks = st.path("numTasks").asInt(0),
                executorRunTimeMs = st.path("executorRunTime").asLong(0L),
                executorCpuTimeMs = st.path("executorCpuTime").asLong(0L) / 1_000_000L,
                jvmGcTimeMs = st.path("jvmGcTime").asLong(0L),
                shuffleReadBytes = st.path("shuffleReadBytes").asLong(0L),
                shuffleWriteBytes = st.path("shuffleWriteBytes").asLong(0L),
                memorySpilledBytes = st.path("memoryBytesSpilled").asLong(0L),
                diskSpilledBytes = st.path("diskBytesSpilled").asLong(0L)
            )
        }

        return RunMetrics(
            wallTimeS = roundedWall,
            numJobs = jobIds.size,
            numStages = wanted.size,
            numTasks = sumField("numTasks"),
            executorRunTimeMs = sumField("executorRunTime"),
            // executorCpuTime comes in nanoseconds from the REST API; convert to ms
            executorCpuTimeMs = sumField("executorCpuTime") / 1_000_000L,
            jvmGcTimeMs = sumField("jvmGcTime"),
            peakExecutionMemoryBytes = sumField("peakExecutionMemory"),
            inputBytes = sumField("inputBytes"),
            outputBytes = sumField("outputBytes"),
            shuffleReadBytes = sumField("shuffleReadBytes"),
            shuffleWriteBytes = sumField("shuffleWriteBytes"),
            shuffleReadRecords = sumField("shuffleReadRecords"),
            shuffleWriteRecords = sumField("shuffleWriteRecords"),
            memorySpilledBytes = sumField("memoryBytesSpilled"),
            diskSpilledBytes = sumField("diskBytesSpilled"),
            stages = stageSummaries
        )
    }

    /**
     * Generates a formatted table report for the console.
     */
    fun report(names: List<String> = runs.keys.toList()): String {
        if (names.isEmpty()) return "(Sem execuções registadas no SparkProfiler)"

        val headers = listOf(
            "run", "wall_time_s", "num_stages", "num_tasks",
            "exec_run_ms", "exec_cpu_ms", "jvm_gc_ms",
            "input", "output", "shuffle_read", "shuffle_write",
            "mem_spill", "disk_spill"
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
                fmtBytes(m.diskSpilledBytes)
            )
        }

        return formatTable(headers, rows)
    }

    /**
     * Generates a Markdown report for quick inclusion in the report or documentation.
     */
    fun reportMarkdown(names: List<String> = runs.keys.toList()): String {
        if (names.isEmpty()) return "*(Sem execuções)*"

        val headers = listOf(
            "Run", "Wall Time (s)", "Stages", "Tasks", "Exec Time (ms)", "CPU Time (ms)",
            "GC Time (ms)", "Input", "Output", "Shuffle Read", "Shuffle Write", "Mem Spill", "Disk Spill"
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
                fmtBytes(m.diskSpilledBytes)
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
        val m = runs[runName] ?: return "Execução '$runName' não encontrada."
        if (m.stages.isEmpty()) return "Sem detalhes de stages para '$runName'."

        val headers = listOf(
            "stage_id", "num_tasks", "run_time_ms", "cpu_time_ms",
            "gc_time_ms", "shuffle_read", "shuffle_write", "mem_spill"
        )
        val rows = m.stages.map { st ->
            listOf(
                st.stageId.toString(),
                st.numTasks.toString(),
                st.executorRunTimeMs.toString(),
                st.executorCpuTimeMs.toString(),
                st.jvmGcTimeMs.toString(),
                fmtBytes(st.shuffleReadBytes),
                fmtBytes(st.shuffleWriteBytes),
                fmtBytes(st.memorySpilledBytes)
            )
        }
        return formatTable(headers, rows)
    }

    /**
     * Compares two executions (baseline vs optimized), showing the percentage change.
     */
    fun compare(before: String, after: String): String {
        val b = runs[before]?.toMap() ?: throw IllegalArgumentException("Run '$before' não encontrada")
        val a = runs[after]?.toMap() ?: throw IllegalArgumentException("Run '$after' não encontrada")

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
        val file = File(filePath)
        file.parentFile?.mkdirs()
        mapper.writeValue(file, runs)
        println("[SparkProfiler] Relatório detalhado exportado para: ${file.absolutePath}")
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
