package compar.profiler

/**
 * Aggregated metrics of one execution (operation) recorded by the profiler.
 */
data class RunMetrics(
    val wallTimeS: Double,
    val numJobs: Int = 0,
    val numStages: Int = 0,
    val numTasks: Long = 0L,
    val executorRunTimeMs: Long = 0L,
    val executorCpuTimeMs: Long = 0L,
    val jvmGcTimeMs: Long = 0L,
    val peakExecutionMemoryBytes: Long = 0L,
    val inputBytes: Long = 0L,
    val outputBytes: Long = 0L,
    val shuffleReadBytes: Long = 0L,
    val shuffleWriteBytes: Long = 0L,
    val shuffleReadRecords: Long = 0L,
    val shuffleWriteRecords: Long = 0L,
    val memorySpilledBytes: Long = 0L,
    val diskSpilledBytes: Long = 0L,
    val maxTaskDurationMs: Long = 0L,
    val medianTaskDurationMs: Long = 0L,
    val stages: List<StageSummary> = emptyList()
) {
    val skewRatio: Double
        get() = if (medianTaskDurationMs > 0) maxTaskDurationMs.toDouble() / medianTaskDurationMs.toDouble() else 1.0

    val hasSignificantSkew: Boolean
        get() = stages.any { it.hasSignificantSkew }

    fun formatSkew(): String =
        if (medianTaskDurationMs > 0) String.format("%.1fx", skewRatio) else "-"
}

/**
 * Per-stage metrics summary for diagnosing bottlenecks and skew.
 */
data class StageSummary(
    val stageId: Int,
    val name: String,
    val numTasks: Int,
    val executorRunTimeMs: Long,
    val executorCpuTimeMs: Long,
    val jvmGcTimeMs: Long,
    val shuffleReadBytes: Long,
    val shuffleWriteBytes: Long,
    val memorySpilledBytes: Long,
    val diskSpilledBytes: Long,
    val peakExecutionMemoryBytes: Long = 0L,
    val maxTaskDurationMs: Long = 0L,
    val medianTaskDurationMs: Long = 0L
) {
    val skewRatio: Double
        get() = if (medianTaskDurationMs > 0) maxTaskDurationMs.toDouble() / medianTaskDurationMs.toDouble() else 1.0

    val hasSignificantSkew: Boolean
        get() = medianTaskDurationMs > 0 && skewRatio >= SKEW_WARNING_THRESHOLD

    fun formatSkew(): String {
        if (medianTaskDurationMs <= 0) return "-"
        val flag = if (hasSignificantSkew) " (!)" else ""
        return String.format("%.1fx%s", skewRatio, flag)
    }

    companion object {
        const val SKEW_WARNING_THRESHOLD = 3.0
    }
}

