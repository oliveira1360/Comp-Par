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
     * Creates or gets the SparkSession configured for local or cluster execution.
     */
    fun getSparkSession(
        masterUrl: String? = null,
        appName: String = "NYC-Taxi-Analytics"
    ): SparkSession {
        var builder = SparkSession.builder().appName(appName)
        if (!masterUrl.isNullOrBlank()) {
            builder = builder.master(masterUrl)
        }

        // Memory settings and recommended optimizations
        builder = builder
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "2g")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.sql.adaptive.enabled", "true")

        val spark = builder.getOrCreate()
        // Set the log level to WARN to keep the console clean and readable
        spark.sparkContext().setLogLevel("WARN")
        return spark
    }

    /**
     * Loads the zone lookup table (taxi_zone_lookup.csv).
     */
    fun loadZoneLookup(spark: SparkSession, lookupPath: String): Dataset<Row> {
        println("\n[Zones] Loading zone lookup table from: $lookupPath")
        return spark.read()
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(lookupPath)
            .withColumn("LocationID", sf.col("LocationID").cast(DataTypes.IntegerType))
    }
}
