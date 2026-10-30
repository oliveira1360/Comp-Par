package compar.ops.o1

import compar.ops.Analysis
import compar.ops.AnalysisInput
import compar.ops.Op
import compar.ops.VersionInfo
import compar.ops.common.TripExprs
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions as sf

/**
 * O1 — Deteção de Anomalias de Tarifa (Fare Anomaly Detection):
 * 1. Calcula fare por km para cada viagem.
 * 2. Por bucket (PULocationID, hour_of_day), calcula média e desvio padrão (σ).
 * 3. Sinaliza viagens com tarifa por km superior a média + 3σ.
 * 4. Compara taxas de anomalia entre dias úteis e fins de semana,
 *    discriminadas por payment_type e pickup_borough.
 */
class O1V0Baseline : Analysis {
    override val info = VersionInfo(Op.O1, 0, change = "Baseline gerado por IA")

    private val kmPerMile = 1.60934

    override fun run(input: AnalysisInput): Dataset<Row> {
        println("\n--- [${info.label}] Deteção de Anomalias de Tarifa (Fare Anomaly Detection) ---")

        // 1. Derivar colunas temporais e tarifa unitária por km
        val dfWithMetrics = input.trips
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
        val dfWithBorough = dfFlagged.join(TripExprs.pickupZones(input.zones), "PULocationID")

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
}
