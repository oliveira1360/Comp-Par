package compar.ops

import compar.ops.m1.M1V0Baseline
import compar.ops.m2.M2V0Baseline
import compar.ops.m3.M3V0Baseline
import compar.ops.o1.O1V0Baseline
import compar.ops.o4.O4V0Baseline

/**
 * Catálogo de todas as versões de cada operação.
 * Para adicionar uma versão: criar a classe e acrescentá-la no fim da lista da respetiva operação.
 * A posição na lista tem de coincidir com o número da versão (validado no arranque).
 */
object Registry {

    // As versões ainda por implementar (TODO()) ficam comentadas: descomentar quando estiverem prontas,
    // caso contrário a execução por defeito (versão mais recente) falha.
    private val ingestions: List<Ingestion> = listOf(
        M1V0Baseline(), // M1V1(),
    )

    private val analyses: Map<Op, List<Analysis>> = listOf(
        M2V0Baseline(), // M2V1(),
        M3V0Baseline(), // M3V1(),
        O1V0Baseline(), // O1V1(),
        O4V0Baseline(), // O4V1(),
    ).groupBy { it.info.op }

    init {
        (analyses.values + listOf(ingestions)).forEach { versions ->
            versions.forEachIndexed { i, step ->
                check(step.info.version == i) { "${step.info.label} está na posição $i do Registry" }
            }
        }
    }

    /** version == null → a versão mais recente. */
    fun ingestion(version: Int?): Ingestion = ingestions.pick(Op.M1, version)

    /** version == null → a versão mais recente. */
    fun analysis(op: Op, version: Int?): Analysis = analyses[op].orEmpty().pick(op, version)

    fun all(): List<Step> = ingestions + Op.entries.flatMap { analyses[it].orEmpty() }

    private fun <T : Step> List<T>.pick(op: Op, version: Int?): T = when (version) {
        null -> lastOrNull() ?: error("$op não tem versões registadas")
        else -> getOrNull(version) ?: error("$op não tem v$version (disponíveis: 0..$lastIndex)")
    }
}
