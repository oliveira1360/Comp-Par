package compar.ops.common

import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions as sf

/**
 * Definições semânticas do enunciado, partilhadas por todas as versões.
 * NÃO colocar aqui nada relacionado com performance — isso pertence a cada versão.
 */
object TripExprs {

    /** Duração da viagem em minutos, derivada dos timestamps. */
    val tripDurationMin: Column
        get() = sf.unix_timestamp(sf.col("tpep_dropoff_datetime"))
            .minus(sf.unix_timestamp(sf.col("tpep_pickup_datetime")))
            .divide(60.0)

    /** Tabela de zonas renomeada para juntar pelo local de recolha. */
    fun pickupZones(zones: Dataset<Row>): Dataset<Row> = zones.select(
        sf.col("LocationID").alias("PULocationID"),
        sf.col("Borough").alias("pickup_borough")
    )
}
