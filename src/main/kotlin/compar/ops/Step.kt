package compar.ops

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

/**
 * Operações do enunciado: obrigatórias (M1-M3) e opcionais escolhidas (O1, O4).
 */
enum class Op { M1, M2, M3, O1, O4 }

/**
 * Metadados de uma versão de uma operação.
 * Cada versão N parte da versão N-1 e regista o bottleneck observado nela e a alteração aplicada.
 */
data class VersionInfo(
    val op: Op,
    val version: Int,
    val change: String,
    val bottleneck: String = "-"
) {
    /** Nome usado no profiler e nos logs (ex: O1_v2). */
    val label get() = "${op}_v$version"
}

/**
 * Uma versão concreta de uma operação.
 */
interface Step {
    val info: VersionInfo
}

/**
 * M1 — lê o dataset e devolve as viagens limpas.
 */
interface Ingestion : Step {
    fun load(spark: SparkSession, source: Source): Dataset<Row>
}

data class Source(val dataPath: String, val sampleFraction: Double?)

/**
 * M2, M3, O1, O4 — transformam as viagens limpas produzidas pelo M1.
 */
interface Analysis : Step {
    /** Colunas que esta versão espera encontrar nas viagens, além do schema base do M1 v0. */
    val requiredColumns: Set<String> get() = emptySet()

    fun run(input: AnalysisInput): Dataset<Row>
}

data class AnalysisInput(
    val spark: SparkSession,
    val trips: Dataset<Row>,
    val zones: Dataset<Row>,
    val outputDir: String
)
