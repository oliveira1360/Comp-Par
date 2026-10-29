package compar.processor

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

/**
 * Interface principal para os processadores de dados de viagens de táxi (NYC Yellow Taxi).
 *
 * Define as operações obrigatórias (M1, M2, M3) e as operações opcionais escolhidas (O1, O4).
 * Permite alternar facilmente entre a implementação de baseline gerada por IA
 * (AiGeneratedDataProcessor) e futuras implementações otimizadas manualmente (ex: OptimizedDataProcessor)
 * para realização de benchmarks comparativos.
 */
interface DataProcessor {

    /**
     * M1 — Ingestão e Limpeza de Dados:
     * - Carrega os Parquets aplicando cast explícito de tipos.
     * - Aplica amostragem se sampleFraction for fornecido.
     * - Filtra registos inválidos segundo as 5 regras do enunciado.
     * - Reporta estatísticas de limpeza por ano.
     * - Retorna o Dataset<Row> limpo.
     */
    fun runM1IngestionAndCleaning(
        spark: SparkSession,
        dataPath: String,
        sampleFraction: Double? = null
    ): Dataset<Row>

    /**
     * M2 — Análise de Procura Temporal:
     * Para cada triplo (pickup_borough, day_of_week, hour_of_day), calcula:
     * - total de viagens (total_trips)
     * - média de tarifa (mean_fare_amount)
     * - duração média em minutos (mean_trip_duration_min)
     * - distância média em milhas (mean_trip_distance)
     * Grava o resultado particionado por pickup_borough em formato Parquet.
     */
    fun runM2TemporalDemandAnalysis(
        dfClean: Dataset<Row>,
        zonesDf: Dataset<Row>,
        outputDir: String
    ): Dataset<Row>

    /**
     * M3 — Estatísticas ao Nível da Zona:
     * Para cada zona de recolha e mês, calcula média e desvio padrão de fare e distance.
     * Utiliza funções de janela (Window functions) para:
     * - Classificar (rank) zonas por volume mensal dentro de cada borough.
     * - Calcular a quota móvel de 3 meses da zona no seu borough.
     * - Sinalizar zonas cujo volume desvia da média móvel em mais de 1.5 desvios padrão.
     */
    fun runM3ZoneLevelStatistics(
        dfClean: Dataset<Row>,
        zonesDf: Dataset<Row>
    ): Dataset<Row>

    /**
     * O1 — Deteção de Anomalias de Tarifa (Fare Anomaly Detection):
     * - Para cada bucket (PULocationID, hour_of_day), calcula média e desvio padrão (σ)
     *   da tarifa por distância (fare_amount / trip_distance).
     * - Sinaliza viagens em que a tarifa por km excede média + 3σ.
     * - Compara a taxa de anomalias entre dias úteis (weekday) e fins de semana (weekend),
     *   discriminada por método de pagamento (payment_type) e borough de recolha.
     */
    fun runO1FareAnomalyDetection(
        dfClean: Dataset<Row>,
        zonesDf: Dataset<Row>
    ): Dataset<Row>

    /**
     * O4 — Classificação de Tipo de Viagem (Trip-type Classification):
     * - Calcula, por bucket (PULocationID, hour_of_day), as frações de:
     *   * viagens para o aeroporto (RatecodeID in {2, 3})
     *   * tarifas negociadas (RatecodeID = 5)
     *   * viagens em grupo (passenger_count >= 4)
     * - Classifica cada bucket como 'residential', 'commercial', 'transit' ou 'mixed'
     *   com base nessas frações e limiares configuráveis.
     * - Junta a classificação de volta à tabela completa de viagens e calcula
     *   estatísticas de tarifa e distância por tipo de zona e por mês.
     */
    fun runO4TripTypeClassification(
        dfClean: Dataset<Row>,
        airportThreshold: Double = 0.10,
        groupThreshold: Double = 0.08,
        commercialMaxGroup: Double = 0.04
    ): Dataset<Row>
}
