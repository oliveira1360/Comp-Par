package compar.ops

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

/**
 * M1 — reads the dataset and returns the cleaned trips.
 */
interface DataLoader {
    val tag: String
        get() = javaClass.simpleName

    fun load(spark: SparkSession, dataPath: String, sampleFraction: Double?): Dataset<Row>
}

/**
 * M2, M3, O1, O4 — transform the cleaned trips produced by M1.
 */
interface Analysis {
    val tag: String
        get() = javaClass.simpleName

    fun run(input: AnalysisInput): Dataset<Row>
}

data class AnalysisInput(
    val spark: SparkSession,
    val trips: Dataset<Row>,
    val zones: Dataset<Row>,
    val outputDir: String
)
