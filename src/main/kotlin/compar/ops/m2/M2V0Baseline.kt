package compar.ops.m2

import compar.ops.Analysis
import compar.ops.AnalysisInput
import compar.ops.Op
import compar.ops.VersionInfo
import compar.ops.common.TripExprs
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions as sf

/**
 * M2 — Análise de Procura Temporal:
 * Para cada triplo (pickup_borough, day_of_week, hour_of_day), calcula:
 * - total de viagens (total_trips)
 * - média de fare_amount (mean_fare_amount)
 * - duração média da viagem em minutos (mean_trip_duration_min)
 * - distância média (mean_trip_distance)
 * Grava o resultado particionado por pickup_borough em formato Parquet.
 */
class M2V0Baseline : Analysis {
    override val info = VersionInfo(Op.M2, 0, change = "Baseline gerado por IA")

    override fun run(input: AnalysisInput): Dataset<Row> {
        println("\n--- [${info.label}] Análise de Procura Temporal ---")

        val m2Df = input.trips
            .join(TripExprs.pickupZones(input.zones), "PULocationID")
            .withColumn("day_of_week", sf.dayofweek(sf.col("tpep_pickup_datetime")))
            .withColumn("hour_of_day", sf.hour(sf.col("tpep_pickup_datetime")))
            .withColumn("trip_duration_min", TripExprs.tripDurationMin)
            .groupBy("pickup_borough", "day_of_week", "hour_of_day")
            .agg(
                sf.count("*").alias("total_trips"),
                sf.avg("fare_amount").alias("mean_fare_amount"),
                sf.avg("trip_duration_min").alias("mean_trip_duration_min"),
                sf.avg("trip_distance").alias("mean_trip_distance")
            )
            .orderBy("pickup_borough", "day_of_week", "hour_of_day")

        m2Df.show(20, false)

        val m2OutputPath = "${input.outputDir}/m2_temporal_demand"
        println("[M2] A gravar resultado particionado por pickup_borough em: $m2OutputPath")
        m2Df.write()
            .mode(SaveMode.Overwrite)
            .partitionBy("pickup_borough")
            .parquet(m2OutputPath)

        return m2Df
    }
}
