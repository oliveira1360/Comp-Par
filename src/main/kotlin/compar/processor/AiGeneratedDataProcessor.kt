package compar.processor

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions as sf
import org.apache.spark.sql.types.DataTypes

/**
 * Implementação Baseline de referência gerada por IA (AiGeneratedDataProcessor).
 *
 * Contém a implementação direta das operações obrigatórias (M1, M2, M3) e opcionais (O1, O4),
 * servindo como ponto de partida (baseline) para posterior perfilagem (profiling),
 * identificação de estrangulamentos e otimização iterativa.
 */
class AiGeneratedDataProcessor : DataProcessor {

    /**
     * M1 — Ingestão e Limpeza de Dados:
     * 1. Carrega os Parquets de viagens com cast explícito de tipos.
     * 2. Aplica amostragem se sampleFraction for fornecido.
     * 3. Identifica e contabiliza registos inválidos segundo as 5 regras do enunciado:
     *    - fare_amount <= 0
     *    - trip_distance <= 0
     *    - passenger_count == 0 ou > 6 (ou nulo)
     *    - dropoff <= pickup (ou nulos)
     *    - PULocationID ou DOLocationID fora de [1, 263]
     * 4. Reporta estatísticas de limpeza por ano.
     * 5. Retorna o DataFrame limpo.
     */
    override fun runM1IngestionAndCleaning(
        spark: SparkSession,
        dataPath: String,
        sampleFraction: Double?
    ): Dataset<Row> {
        println("\n[M1] A carregar dados de: $dataPath")
        val dfRaw = spark.read().parquet(dataPath)

        var df = dfRaw
            .withColumn("tpep_pickup_datetime", sf.col("tpep_pickup_datetime").cast(DataTypes.TimestampType))
            .withColumn("tpep_dropoff_datetime", sf.col("tpep_dropoff_datetime").cast(DataTypes.TimestampType))
            .withColumn("passenger_count", sf.col("passenger_count").cast(DataTypes.IntegerType))
            .withColumn("RatecodeID", sf.col("RatecodeID").cast(DataTypes.IntegerType))
            .withColumn("payment_type", sf.col("payment_type").cast(DataTypes.IntegerType))
            .withColumn("PULocationID", sf.col("PULocationID").cast(DataTypes.IntegerType))
            .withColumn("DOLocationID", sf.col("DOLocationID").cast(DataTypes.IntegerType))
            .withColumn("year", sf.year(sf.col("tpep_pickup_datetime")))

        if (sampleFraction != null && sampleFraction > 0.0 && sampleFraction < 1.0) {
            println(String.format("[M1] A aplicar amostragem de %.1f%%", sampleFraction * 100.0))
            df = df.sample(sampleFraction, 42L)
        }

        val totalRecords = df.count()
        println(String.format("[M1] Total de registos lidos: %,d", totalRecords))

        // Definição dos critérios de invalidade (M1)
        val condInvalidFare = sf.col("fare_amount").leq(0).or(sf.col("fare_amount").isNull)
        val condInvalidDistance = sf.col("trip_distance").leq(0).or(sf.col("trip_distance").isNull)
        val condInvalidPassenger = sf.col("passenger_count").leq(0)
            .or(sf.col("passenger_count").gt(6))
            .or(sf.col("passenger_count").isNull)
        val condInvalidTime = sf.col("tpep_dropoff_datetime").leq(sf.col("tpep_pickup_datetime"))
            .or(sf.col("tpep_pickup_datetime").isNull)
            .or(sf.col("tpep_dropoff_datetime").isNull)
        val condInvalidLocation = sf.not(sf.col("PULocationID").between(1, 263))
            .or(sf.not(sf.col("DOLocationID").between(1, 263)))
            .or(sf.col("PULocationID").isNull)
            .or(sf.col("DOLocationID").isNull)

        // Adicionar colunas indicadoras (0/1) para contabilização por critério
        val dfFlagged = df
            .withColumn("rem_fare", sf.`when`(condInvalidFare, 1).otherwise(0))
            .withColumn("rem_distance", sf.`when`(condInvalidDistance, 1).otherwise(0))
            .withColumn("rem_passenger", sf.`when`(condInvalidPassenger, 1).otherwise(0))
            .withColumn("rem_time", sf.`when`(condInvalidTime, 1).otherwise(0))
            .withColumn("rem_location", sf.`when`(condInvalidLocation, 1).otherwise(0))

        // Estatísticas por ano de remoção por critério (requisito M1)
        println("\n--- [M1] Estatísticas de Limpeza por Ano (Registos Removidos) ---")
        val cleaningStats = dfFlagged
            .groupBy("year")
            .agg(
                sf.count("*").alias("total_records"),
                sf.sum("rem_fare").alias("invalid_fare"),
                sf.sum("rem_distance").alias("invalid_distance"),
                sf.sum("rem_passenger").alias("invalid_passenger"),
                sf.sum("rem_time").alias("invalid_time"),
                sf.sum("rem_location").alias("invalid_location")
            )
            .orderBy("year")

        cleaningStats.show(false)

        // Filtrar o DataFrame apenas com os registos estritamente válidos
        val condValidAll = sf.not(condInvalidFare)
            .and(sf.not(condInvalidDistance))
            .and(sf.not(condInvalidPassenger))
            .and(sf.not(condInvalidTime))
            .and(sf.not(condInvalidLocation))

        val dfClean = dfFlagged
            .filter(condValidAll)
            .drop("rem_fare", "rem_distance", "rem_passenger", "rem_time", "rem_location")

        val validRecords = dfClean.count()
        val removedRecords = totalRecords - validRecords
        val pctRemoved = if (totalRecords > 0) (removedRecords.toDouble() / totalRecords.toDouble() * 100.0) else 0.0
        println(String.format("[M1] Registos válidos: %,d (%.2f%%)", validRecords, 100.0 - pctRemoved))
        println(String.format("[M1] Registos removidos no total: %,d (%.2f%%)", removedRecords, pctRemoved))

        return dfClean
    }

    /**
     * M2 — Análise de Procura Temporal:
     * Para cada triplo (pickup_borough, day_of_week, hour_of_day), calcula:
     * - total de viagens (total_trips)
     * - média de fare_amount (mean_fare_amount)
     * - duração média da viagem em minutos (mean_trip_duration_min)
     * - distância média (mean_trip_distance)
     * Grava o resultado particionado por pickup_borough em formato Parquet.
     */
    override fun runM2TemporalDemandAnalysis(
        dfClean: Dataset<Row>,
        zonesDf: Dataset<Row>,
        outputDir: String
    ): Dataset<Row> {
        println("\n--- [M2] Análise de Procura Temporal ---")

        val zonesPickup = zonesDf.select(
            sf.col("LocationID").alias("PULocationID"),
            sf.col("Borough").alias("pickup_borough")
        )

        val durationMinExpr = sf.unix_timestamp(sf.col("tpep_dropoff_datetime"))
            .minus(sf.unix_timestamp(sf.col("tpep_pickup_datetime")))
            .divide(60.0)

        val m2Df = dfClean
            .join(zonesPickup, "PULocationID")
            .withColumn("day_of_week", sf.dayofweek(sf.col("tpep_pickup_datetime")))
            .withColumn("hour_of_day", sf.hour(sf.col("tpep_pickup_datetime")))
            .withColumn("trip_duration_min", durationMinExpr)
            .groupBy("pickup_borough", "day_of_week", "hour_of_day")
            .agg(
                sf.count("*").alias("total_trips"),
                sf.avg("fare_amount").alias("mean_fare_amount"),
                sf.avg("trip_duration_min").alias("mean_trip_duration_min"),
                sf.avg("trip_distance").alias("mean_trip_distance")
            )
            .orderBy("pickup_borough", "day_of_week", "hour_of_day")

        m2Df.show(20, false)

        val m2OutputPath = "$outputDir/m2_temporal_demand"
        println("[M2] A gravar resultado particionado por pickup_borough em: $m2OutputPath")
        m2Df.write()
            .mode(SaveMode.Overwrite)
            .partitionBy("pickup_borough")
            .parquet(m2OutputPath)

        return m2Df
    }

    /**
     * M3 — Estatísticas ao Nível da Zona:
     * Para cada zona de recolha e mês, calcula média e desvio padrão de fare_amount e trip_distance.
     * Usa funções de janela (Window functions) para:
     * 1. Classificar (rank) zonas por volume mensal de viagens dentro de cada borough.
     * 2. Calcular a quota móvel de 3 meses de viagens de cada zona dentro do seu borough.
     * 3. Sinalizar (flag) zonas cujo volume no mês corrente desvia da sua própria média móvel em mais de 1.5 desvios padrão.
     */
    override fun runM3ZoneLevelStatistics(
        dfClean: Dataset<Row>,
        zonesDf: Dataset<Row>
    ): Dataset<Row> {
        println("\n--- [M3] Estatísticas ao Nível da Zona ---")

        val zonesPickup = zonesDf.select(
            sf.col("LocationID").alias("PULocationID"),
            sf.col("Borough").alias("pickup_borough"),
            sf.col("Zone").alias("pickup_zone")
        )

        val monthlyZoneStats = dfClean
            .join(zonesPickup, "PULocationID")
            .withColumn("year_month", sf.date_format(sf.col("tpep_pickup_datetime"), "yyyy-MM"))
            .groupBy("PULocationID", "pickup_borough", "pickup_zone", "year_month")
            .agg(
                sf.count("*").alias("monthly_trip_volume"),
                sf.avg("fare_amount").alias("mean_fare_amount"),
                sf.stddev("fare_amount").alias("stddev_fare_amount"),
                sf.avg("trip_distance").alias("mean_trip_distance"),
                sf.stddev("trip_distance").alias("stddev_trip_distance")
            )

        val wBoroughMonthRank = Window
            .partitionBy("pickup_borough", "year_month")
            .orderBy(sf.col("monthly_trip_volume").desc())

        val wBoroughMonthTotal = Window
            .partitionBy("pickup_borough", "year_month")

        val wZoneRolling3M = Window
            .partitionBy("PULocationID")
            .orderBy("year_month")
            .rowsBetween(-2, 0)

        val m3Df = monthlyZoneStats
            .withColumn("borough_volume_rank", sf.rank().over(wBoroughMonthRank))
            .withColumn("borough_monthly_volume", sf.sum("monthly_trip_volume").over(wBoroughMonthTotal))
            .withColumn(
                "monthly_trip_share",
                sf.`when`(
                    sf.col("borough_monthly_volume").gt(0),
                    sf.col("monthly_trip_volume").divide(sf.col("borough_monthly_volume"))
                ).otherwise(0.0)
            )
            .withColumn("rolling_3m_trip_share", sf.avg("monthly_trip_share").over(wZoneRolling3M))
            .withColumn("rolling_mean_volume", sf.avg("monthly_trip_volume").over(wZoneRolling3M))
            .withColumn("rolling_stddev_volume", sf.stddev("monthly_trip_volume").over(wZoneRolling3M))
            .withColumn(
                "volume_deviation_flag",
                sf.coalesce(
                    sf.abs(sf.col("monthly_trip_volume").minus(sf.col("rolling_mean_volume")))
                        .gt(sf.col("rolling_stddev_volume").multiply(1.5)),
                    sf.lit(false)
                )
            )
            .drop("borough_monthly_volume", "monthly_trip_share")
            .orderBy(sf.col("year_month"), sf.col("pickup_borough"), sf.col("borough_volume_rank"))

        m3Df.show(20, false)

        val flaggedCount = m3Df.filter(sf.col("volume_deviation_flag").equalTo(true)).count()
        println(String.format("[M3] Total de zonas-mês com desvio > 1.5 std: %,d", flaggedCount))

        return m3Df
    }

    /**
     * O1 — Deteção de Anomalias de Tarifa (Fare Anomaly Detection):
     * 1. Calcula fare por km (ou por milha) para cada viagem.
     * 2. Por bucket (PULocationID, hour_of_day), calcula média e desvio padrão (σ).
     * 3. Sinaliza viagens com tarifa por km superior a média + 3σ.
     * 4. Compara taxas de anomalia entre dias úteis e fins de semana,
     *    discriminadas por payment_type e pickup_borough.
     */
    override fun runO1FareAnomalyDetection(
        dfClean: Dataset<Row>,
        zonesDf: Dataset<Row>
    ): Dataset<Row> {
        println("\n--- [O1] Deteção de Anomalias de Tarifa (Fare Anomaly Detection) ---")

        val kmPerMile = 1.60934

        // 1. Derivar colunas temporais e tarifa unitária por km
        val dfWithMetrics = dfClean
            .withColumn("hour_of_day", sf.hour(sf.col("tpep_pickup_datetime")))
            .withColumn("day_of_week", sf.dayofweek(sf.col("tpep_pickup_datetime")))
            .withColumn(
                "day_type",
                sf.`when`(sf.col("day_of_week").isin(1, 7), sf.lit("Weekend")).otherwise(sf.lit("Weekday"))
            )
            .withColumn(
                "fare_per_km",
                sf.col("fare_amount").divide(sf.col("trip_distance").multiply(kmPerMile))
            )

        // 2. Window partition por bucket (PULocationID, hour_of_day) para calcular média e sigma
        val wBucket = Window.partitionBy("PULocationID", "hour_of_day")

        val dfWithBucketStats = dfWithMetrics
            .withColumn("mean_fare_per_km", sf.avg("fare_per_km").over(wBucket))
            .withColumn("stddev_fare_per_km", sf.stddev("fare_per_km").over(wBucket))

        // 3. Sinalizar viagens anómalas: fare_per_km > mean + 3*stddev
        val thresholdExpr = sf.col("mean_fare_per_km").plus(sf.col("stddev_fare_per_km").multiply(3.0))
        val isAnomalyExpr = sf.coalesce(
            sf.col("stddev_fare_per_km").gt(0.0).and(sf.col("fare_per_km").gt(thresholdExpr)),
            sf.lit(false)
        )

        val dfFlagged = dfWithBucketStats.withColumn("is_anomaly", isAnomalyExpr)

        // 4. Juntar com a tabela de zonas para obter o Borough de recolha
        val zonesPickup = zonesDf.select(
            sf.col("LocationID").alias("PULocationID"),
            sf.col("Borough").alias("pickup_borough")
        )

        val dfWithBorough = dfFlagged.join(zonesPickup, "PULocationID")

        // 5. Comparar taxas de anomalia entre weekday e weekend discriminadas por payment_type e pickup_borough
        val anomalyRatesDf = dfWithBorough
            .groupBy("pickup_borough", "payment_type", "day_type")
            .agg(
                sf.count("*").alias("total_trips"),
                sf.sum(sf.`when`(sf.col("is_anomaly"), 1).otherwise(0)).alias("anomaly_trips"),
                sf.round(
                    sf.avg(sf.`when`(sf.col("is_anomaly"), 1.0).otherwise(0.0)).multiply(100.0),
                    2
                ).alias("anomaly_rate_pct")
            )
            .orderBy("pickup_borough", "payment_type", "day_type")

        anomalyRatesDf.show(30, false)

        val totalAnomalies = dfFlagged.filter(sf.col("is_anomaly").equalTo(true)).count()
        println(String.format("[O1] Total de viagens sinalizadas como anomalias (> 3σ): %,d", totalAnomalies))

        return anomalyRatesDf
    }

    /**
     * O4 — Classificação de Tipo de Viagem (Trip-Type Classification):
     * 1. Calcula frações de aeroporto, negociadas e em grupo por bucket (PULocationID, hour_of_day).
     * 2. Classifica cada bucket em 'residential', 'commercial', 'transit' ou 'mixed'.
     * 3. Junta a classificação de volta à tabela completa de viagens.
     * 4. Calcula estatísticas de tarifa e distância por tipo de zona e por mês.
     */
    override fun runO4TripTypeClassification(
        dfClean: Dataset<Row>,
        airportThreshold: Double,
        groupThreshold: Double,
        commercialMaxGroup: Double
    ): Dataset<Row> {
        println("\n--- [O4] Classificação de Tipo de Viagem (Trip-Type Classification) ---")

        val dfPrepared = dfClean
            .withColumn("hour_of_day", sf.hour(sf.col("tpep_pickup_datetime")))
            .withColumn("year_month", sf.date_format(sf.col("tpep_pickup_datetime"), "yyyy-MM"))

        // 1. Agregação por bucket (PULocationID, hour_of_day) para calcular frações
        val bucketFractions = dfPrepared
            .groupBy("PULocationID", "hour_of_day")
            .agg(
                sf.count("*").alias("bucket_total_trips"),
                sf.avg(sf.`when`(sf.col("RatecodeID").isin(2, 3), 1.0).otherwise(0.0)).alias("frac_airport"),
                sf.avg(sf.`when`(sf.col("RatecodeID").equalTo(5), 1.0).otherwise(0.0)).alias("frac_negotiated"),
                sf.avg(sf.`when`(sf.col("passenger_count").geq(4), 1.0).otherwise(0.0)).alias("frac_group")
            )

        // 2. Classificação de cada bucket com base nos limiares configuráveis
        val zoneTypeExpr = sf.`when`(
            sf.col("frac_airport").geq(airportThreshold),
            sf.lit("transit")
        ).`when`(
            sf.col("frac_group").geq(groupThreshold),
            sf.lit("residential")
        ).`when`(
            sf.col("frac_group").leq(commercialMaxGroup),
            sf.lit("commercial")
        ).otherwise(
            sf.lit("mixed")
        )

        val classifiedBuckets = bucketFractions
            .withColumn("zone_type", zoneTypeExpr)
            .select("PULocationID", "hour_of_day", "zone_type")

        // 3. Juntar a classificação de volta à tabela de viagens
        val tripsWithZoneType = dfPrepared
            .join(classifiedBuckets, arrayOf("PULocationID", "hour_of_day"))

        // 4. Calcular estatísticas de tarifa e distância por tipo de zona e mês
        val o4Stats = tripsWithZoneType
            .groupBy("year_month", "zone_type")
            .agg(
                sf.count("*").alias("total_trips"),
                sf.avg("fare_amount").alias("mean_fare_amount"),
                sf.stddev("fare_amount").alias("stddev_fare_amount"),
                sf.avg("trip_distance").alias("mean_trip_distance"),
                sf.stddev("trip_distance").alias("stddev_trip_distance"),
                sf.avg("total_amount").alias("mean_total_amount")
            )
            .orderBy("year_month", "zone_type")

        o4Stats.show(20, false)

        return o4Stats
    }
}
