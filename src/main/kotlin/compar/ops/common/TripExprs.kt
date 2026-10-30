package compar.ops.common

import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions as sf

// Semantic definitions from the assignment, shared by all versions.
// Do NOT put anything performance-related here: that belongs in each version.

/** Trip duration in minutes, derived from the timestamps. */
fun tripDurationMin(): Column =
    sf.unix_timestamp(sf.col("tpep_dropoff_datetime"))
        .minus(sf.unix_timestamp(sf.col("tpep_pickup_datetime")))
        .divide(60.0)

/** Zone table renamed for joining on the pickup location. */
fun pickupZones(zones: Dataset<Row>): Dataset<Row> = zones.select(
    sf.col("LocationID").alias("PULocationID"),
    sf.col("Borough").alias("pickup_borough")
)
