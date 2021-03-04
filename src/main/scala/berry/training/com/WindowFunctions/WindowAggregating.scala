package berry.training.com.WindowFunctions

import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object WindowAggregating extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Start Window Aggregating!!!")

    // Create Spark Session
    val spark = SparkSession
      .builder()
      .appName("Aggregating demo")
      .master("local[3]")
      .getOrCreate()

    val summaryDF = spark.read.parquet("data/summary.parquet")

    summaryDF.show()

    val runningTotalWindow = Window
      .partitionBy("Country")
      .orderBy("WeekNumber")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    summaryDF
      .withColumn("Running_Total", sum("InvoiceValue").over(runningTotalWindow))
      .show()

    logger.info("Stop  Window Aggregating!!!")
    spark.stop()
  }

}
