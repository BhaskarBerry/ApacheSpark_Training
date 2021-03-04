package berry.training.com.DataSink

import org.apache.log4j.Logger
import org.apache.spark.sql._

object DataSinkDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    // Create Spark Session
    val spark = SparkSession
      .builder()
      .appName("Spark Schema ")
      .master("local[3]")
      .getOrCreate()

    val flightTimeParquetDF = spark.read
      .format("parquet")
      .option("path", "data/flight*.parquet")
      .load()

    logger.info(
      "Number of partition Before: " + flightTimeParquetDF.rdd.getNumPartitions)

    import org.apache.spark.sql.functions.spark_partition_id
    flightTimeParquetDF.groupBy(spark_partition_id()).count().show()

    val partitionedDF = flightTimeParquetDF.repartition(5)

    logger.info(
      "Number of partition After: " + partitionedDF.rdd.getNumPartitions)

    partitionedDF.groupBy(spark_partition_id()).count().show()
    /*
    partitionedDF.write
      .format("avro")
      .mode(SaveMode.Overwrite)
      .option("path", "dataSink/avro/")
      .save()
     */
    flightTimeParquetDF.write
      .format("json")
      .mode(SaveMode.Overwrite)
      .option("path", "dataSink/json/")
      .partitionBy("OP_CARRIER", "ORIGIN")
      .option("maxRecordsPerFile", 10000)
      .save()
    //    logger.info("CSV Schema:" + flightTimeParquetDF.schema.simpleString)

    logger.info("Stop Spark App !!!!")
    spark.stop()
  }

}
