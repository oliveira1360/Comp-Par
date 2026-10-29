package compar.util

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions as sf
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.Metadata

object SparkUtils {

    /**
     * Cria ou obtém a SparkSession configurada para execução local ou no cluster.
     */
    fun getSparkSession(
        masterUrl: String? = null,
        appName: String = "NYC-Taxi-Analytics"
    ): SparkSession {
        var builder = SparkSession.builder().appName(appName)
        if (!masterUrl.isNullOrBlank()) {
            builder = builder.master(masterUrl)
        }

        // Configurações de memória e otimizações recomendadas
        builder = builder
            .config("spark.driver.memory", "1536m")
            .config("spark.executor.memory", "1024m")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.sql.adaptive.enabled", "true")

        val spark = builder.getOrCreate()
        // Define o log level para WARN para manter a consola limpa e legível
        spark.sparkContext().setLogLevel("WARN")
        return spark
    }

    /**
     * Define o esquema explícito para os dados do TLC Yellow Taxi.
     * Evita a necessidade de inferSchema e garante tipos consistentes.
     */
    fun getRawSchema(): StructType = StructType(
        arrayOf(
            StructField.apply("tpep_pickup_datetime", DataTypes.TimestampType, true, Metadata.empty()),
            StructField.apply("tpep_dropoff_datetime", DataTypes.TimestampType, true, Metadata.empty()),
            StructField.apply("PULocationID", DataTypes.IntegerType, true, Metadata.empty()),
            StructField.apply("DOLocationID", DataTypes.IntegerType, true, Metadata.empty()),
            StructField.apply("passenger_count", DataTypes.DoubleType, true, Metadata.empty()),
            StructField.apply("trip_distance", DataTypes.DoubleType, true, Metadata.empty()),
            StructField.apply("RatecodeID", DataTypes.DoubleType, true, Metadata.empty()),
            StructField.apply("payment_type", DataTypes.LongType, true, Metadata.empty()),
            StructField.apply("fare_amount", DataTypes.DoubleType, true, Metadata.empty()),
            StructField.apply("tip_amount", DataTypes.DoubleType, true, Metadata.empty()),
            StructField.apply("total_amount", DataTypes.DoubleType, true, Metadata.empty())
        )
    )

    /**
     * Carrega a tabela de lookup de zonas (taxi_zone_lookup.csv).
     */
    fun loadZoneLookup(spark: SparkSession, lookupPath: String): Dataset<Row> {
        println("\n[Zones] A carregar tabela de zonas de: $lookupPath")
        return spark.read()
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(lookupPath)
            .withColumn("LocationID", sf.col("LocationID").cast(DataTypes.IntegerType))
    }
}
