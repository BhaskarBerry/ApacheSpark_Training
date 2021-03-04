package berry.training.com.SparkSQL

import org.apache.log4j.Logger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSchema extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    // Create Spark Session
    val spark = SparkSession
      .builder()
      .appName("Spark Schema ")
      .master("local[3]")
      .getOrCreate()

    val flightSchemaStruct = StructType(
      List(
        StructField("FL_DATE", DateType),
        StructField("OP_CARRIER", StringType),
        StructField("OP_CARRIER_FL_NUM", IntegerType),
        StructField("ORIGIN", StringType),
        StructField("ORIGIN_CITY_NAME", StringType),
        StructField("DEST", StringType),
        StructField("DEST_CITY_NAME", StringType),
        StructField("CRS_DEP_TIME", IntegerType),
        StructField("DEP_TIME", IntegerType),
        StructField("WHEELS_ON", IntegerType),
        StructField("TAXI_IN", IntegerType),
        StructField("CRS_ARR_TIME", IntegerType),
        StructField("ARR_TIME", IntegerType),
        StructField("CANCELLED", IntegerType),
        StructField("DISTANCE", IntegerType)
      ))

    // 2nd way
    val flightSchemaDDL = "FL_DATE DATE, OP_CARRIER STRING,OP_CARRIER_FL_NUM INT,ORIGIN STRING,ORIGIN_CITY_NAME STRING," +
      "DEST STRING,DEST_CITY_NAME STRING,CRS_DEP_TIME INT,DEP_TIME INT,WHEELS_ON INT,TAXI_IN INT,CRS_ARR_TIME  INT" +
      ",ARR_TIME INT,CANCELLED INT,DISTANCE INT"

    val flightTimeCSVDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("path", "data/flight*.csv")
      //      .option("inferSchema", "true")
      .option("mode", "FAIL/FAST")
      .schema(flightSchemaStruct)
      .option("dateFormat", "M/d/y")
      .load()

    flightTimeCSVDF.show(5)
    logger.info("CSV Schema:" + flightTimeCSVDF.schema.simpleString)

    val flightTimeJSONDF = spark.read
      .format("json")
      .option("path", "data/flight*.json")
      .schema(flightSchemaDDL)
      .option("dateFormat", "M/d/y")
      .load()

    flightTimeJSONDF.show(5)
    logger.info("CSV Schema:" + flightTimeJSONDF.schema.simpleString)

    val flightTimeParquetDF = spark.read
      .format("parquet")
      .option("path", "data/flight*.parquet")
      .load()

    flightTimeParquetDF.show(5)
    logger.info("CSV Schema:" + flightTimeParquetDF.schema.simpleString)

    logger.info("Stop Spark App !!!!")
    spark.stop()
  }

}
