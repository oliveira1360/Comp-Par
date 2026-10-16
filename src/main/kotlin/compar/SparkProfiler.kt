package compar

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.SparkSession
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID
import kotlin.math.abs
import kotlin.math.roundToLong

/**
 * SparkProfiler.kt
 * ----------------
 * Utilitário sem dependências externas para perfilagem (profiling) de jobs Spark em Kotlin.
 *
 * Recorre apenas ao que o próprio Spark já oferece nativamente:
 * 1. `SparkContext.setJobGroup(...)` para etiquetar os jobs lançados dentro de um bloco `profile(...)`.
 * 2. A REST API pública da Spark Application UI
 *    (`http://<driver-host>:<ui-port>/api/v1/applications/<app-id>/stages`),
 *    para recolher métricas de shuffle, spill, CPU e runtime por stage.
 */
class SparkProfiler(
    private val spark: SparkSession,
    uiBaseUrlOverride: String? = null
) {
    data class RunMetrics(
        val wallTimeS: Double,
        val numJobs: Int,
        val numStages: Int,
        val numTasks: Long,
        val executorRunTimeMs: Long,
        val executorCpuTimeMs: Long,
        val inputBytes: Long,
        val outputBytes: Long,
        val shuffleReadBytes: Long,
        val shuffleWriteBytes: Long,
        val memorySpilledBytes: Long,
        val diskSpilledBytes: Long
    ) {
        fun toMap(): Map<String, Number> = linkedMapOf(
            "wall_time_s" to wallTimeS,
            "num_jobs" to numJobs,
            "num_stages" to numStages,
            "num_tasks" to numTasks,
            "executor_run_time_ms" to executorRunTimeMs,
            "executor_cpu_time_ms" to executorCpuTimeMs,
            "input_bytes" to inputBytes,
            "output_bytes" to outputBytes,
            "shuffle_read_bytes" to shuffleReadBytes,
            "shuffle_write_bytes" to shuffleWriteBytes,
            "memory_spilled_bytes" to memorySpilledBytes,
            "disk_spilled_bytes" to diskSpilledBytes
        )
    }

    private val sc = spark.sparkContext()
    private val appId: String = sc.applicationId()
    private val uiBaseUrl: String = uiBaseUrlOverride ?: detectUiBaseUrl()
    private val httpClient: HttpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build()
    private val mapper = ObjectMapper()

    val runs: MutableMap<String, RunMetrics> = linkedMapOf()

    private fun detectUiBaseUrl(): String {
        val urlOpt = sc.uiWebUrl()
        val url = if (urlOpt.isDefined) urlOpt.get() else null
        val port = url?.substringAfterLast(":", "4040") ?: "4040"
        return "http://localhost:$port"
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
     * Envolve o bloco de código Spark a medir:
     * ```
     * profiler.profile("M1_ingestion") {
     *     ...
     * }
     * ```
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
                val metrics = collect(groupId, wallTimeS)
                runs[name] = metrics
                println(
                    String.format(
                        "[%s] wall time: %.2fs (%d stages, %d tasks) - see profiler.report() / profiler.compare(...)",
                        name, wallTimeS, metrics.numStages, metrics.numTasks
                    )
                )
            } catch (e: Exception) {
                println(
                    String.format(
                        "[%s] wall time: %.2fs (não foi possível recolher métricas de profiling: %s)",
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

        return RunMetrics(
            wallTimeS = roundedWall,
            numJobs = jobIds.size,
            numStages = wanted.size,
            numTasks = sumField("numTasks"),
            executorRunTimeMs = sumField("executorRunTime"),
            // executorCpuTime vem em nanosegundos na REST API do Spark; convertemos para ms
            executorCpuTimeMs = sumField("executorCpuTime") / 1_000_000L,
            inputBytes = sumField("inputBytes"),
            outputBytes = sumField("outputBytes"),
            shuffleReadBytes = sumField("shuffleReadBytes"),
            shuffleWriteBytes = sumField("shuffleWriteBytes"),
            memorySpilledBytes = sumField("memoryBytesSpilled"),
            diskSpilledBytes = sumField("diskBytesSpilled")
        )
    }

    fun report(names: List<String> = runs.keys.toList()): String {
        if (names.isEmpty()) return "(Sem execuções registadas no SparkProfiler)"

        val headers = listOf(
            "run", "wall_time_s", "num_jobs", "num_stages", "num_tasks",
            "executor_run_time_ms", "executor_cpu_time_ms",
            "input_bytes", "output_bytes", "shuffle_read_bytes",
            "shuffle_write_bytes", "memory_spilled_bytes", "disk_spilled_bytes"
        )

        val rows = names.mapNotNull { name ->
            val m = runs[name] ?: return@mapNotNull null
            listOf(
                name,
                String.format("%.2f", m.wallTimeS),
                m.numJobs.toString(),
                m.numStages.toString(),
                m.numTasks.toString(),
                m.executorRunTimeMs.toString(),
                m.executorCpuTimeMs.toString(),
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

    companion object {
        private val BYTE_KEYS = setOf(
            "input_bytes", "output_bytes", "shuffle_read_bytes",
            "shuffle_write_bytes", "memory_spilled_bytes", "disk_spilled_bytes"
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
