package compar.profiler

/**
 * Métricas agregadas de uma execução (operação) registada pelo profiler.
 */
data class RunMetrics(
    val wallTimeS: Double,
    val numJobs: Int,
    val numStages: Int,
    val numTasks: Long,
    val executorRunTimeMs: Long,
    val executorCpuTimeMs: Long,
    val jvmGcTimeMs: Long,
    val peakExecutionMemoryBytes: Long,
    val inputBytes: Long,
    val outputBytes: Long,
    val shuffleReadBytes: Long,
    val shuffleWriteBytes: Long,
    val shuffleReadRecords: Long,
    val shuffleWriteRecords: Long,
    val memorySpilledBytes: Long,
    val diskSpilledBytes: Long,
    val maxTaskDurationMs: Long = 0L,
    val medianTaskDurationMs: Long = 0L,
    val stages: List<StageSummary> = emptyList()
) {
    /**
     * Converte as métricas escalares para mapa para facilitar exportação ou comparações.
     */
    fun toMap(): Map<String, Number> = linkedMapOf(
        "wall_time_s" to wallTimeS,
        "num_jobs" to numJobs,
        "num_stages" to numStages,
        "num_tasks" to numTasks,
        "executor_run_time_ms" to executorRunTimeMs,
        "executor_cpu_time_ms" to executorCpuTimeMs,
        "jvm_gc_time_ms" to jvmGcTimeMs,
        "peak_execution_memory_bytes" to peakExecutionMemoryBytes,
        "input_bytes" to inputBytes,
        "output_bytes" to outputBytes,
        "shuffle_read_bytes" to shuffleReadBytes,
        "shuffle_write_bytes" to shuffleWriteBytes,
        "shuffle_read_records" to shuffleReadRecords,
        "shuffle_write_records" to shuffleWriteRecords,
        "memory_spilled_bytes" to memorySpilledBytes,
        "disk_spilled_bytes" to diskSpilledBytes,
        "max_task_duration_ms" to maxTaskDurationMs,
        "median_task_duration_ms" to medianTaskDurationMs
    )
}

/**
 * Resumo de métricas por stage individual para diagnóstico de bottlenecks e skew.
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
    val diskSpilledBytes: Long
)
