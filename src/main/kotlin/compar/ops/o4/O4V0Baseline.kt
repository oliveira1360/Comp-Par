package compar.ops.o4

import compar.ops.Analysis
import compar.ops.AnalysisInput
import compar.ops.Op
import compar.ops.VersionInfo
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions as sf

/**
 * O4 — Classificação de Tipo de Viagem (Trip-Type Classification):
 * 1. Calcula frações de aeroporto, negociadas e em grupo por bucket (PULocationID, hour_of_day).
 * 2. Classifica cada bucket em 'residential', 'commercial', 'transit' ou 'mixed'.
 * 3. Junta a classificação de volta à tabela completa de viagens.
 * 4. Calcula estatísticas de tarifa e distância por tipo de zona e por mês.
 */
class O4V0Baseline(
    private val airportThreshold: Double = 0.10,
    private val groupThreshold: Double = 0.08,
    private val commercialMaxGroup: Double = 0.04
) : Analysis {
    override val info = VersionInfo(Op.O4, 0, change = "Baseline gerado por IA")

    override fun run(input: AnalysisInput): Dataset<Row> {
        println("\n--- [${info.label}] Classificação de Tipo de Viagem (Trip-Type Classification) ---")

        val dfPrepared = input.trips
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
