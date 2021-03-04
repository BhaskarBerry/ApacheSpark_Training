package berry.training.com.SparkSQL

import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object HelloSparkSQL extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Starting HelloDataSet!!!!")

    if (args.length == 0) {
      logger.error("There is no filename")
      System.exit(1)
    }

    // Create Spark Session
    val spark = SparkSession
      .builder()
      .appName("Hello DataSet")
      .master("local[3]")
      .getOrCreate()

    import spark.implicits._

    // Read your CSV file
    val surveyDF: Dataset[Row] = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(0))

    surveyDF.createOrReplaceTempView("survey_tbl")

    val countDF = spark.sql(
      "select Country, count(1) as count from survey_tbl where Age < 40 group by Country ")

    logger.info("Count:" + countDF.collect().mkString(","))

    logger.info("Stop Hello Spark!!!!")

    spark.stop()
  }
}
