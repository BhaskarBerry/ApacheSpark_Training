package berry.training.com.WindowFunctions

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object AggregatingData extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    // Create Spark Session
    val spark = SparkSession
      .builder()
      .appName("AggregatingData Demo")
      .master("local[3]")
      .getOrCreate()

    val invoiceDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("data/invoices.csv")

    invoiceDF.show()

    invoiceDF
      .select(
        count("*").as("Count *"),
        sum("Quantity").as("TotalQuantity"),
        avg("UnitPrice").as("AvgPrice"),
        countDistinct("InvoiceNo").as("CountDistinct")
      )
    //      .show()

    invoiceDF
      .selectExpr(
        "count(1) as Count1",
        "count(StockCode) as StockcountFiled",
        "sum(Quantity) as TotalQuantity ",
        "avg(UnitPrice) as AvgPrice "
      )
    //      .show()

    invoiceDF.createOrReplaceTempView("sales")

    val summarySQL =
      spark.sql("""
        |SELECT Country,InvoiceNo,
        |sum(Quantity) as TotalQunatity,
        |round(sum(Quantity * UnitPrice ),2) as InvoiceValue
        |FROM sales
        |GROUP BY Country, InvoiceNo
      """.stripMargin)

    //    summarySQL.show()

    // Using DataFrame group by
    val summaryDF = invoiceDF
      .groupBy("Country", "InvoiceNo")
      .agg(sum("Quantity").as("Total Quantity"),
           round(sum(expr("Quantity * UnitPrice")), 2).as("InvoiceValue"))

    //    summaryDF.show()

    // Using Aggregation group by

    val TotalQuantity = sum("Quantity").as("TotalQuantity")
    val InvoiceValue = expr(
      "round(sum(Quantity * UnitPrice),2) as InvoiceValue")

    val summaryWeekDF = invoiceDF
      .withColumn("InvoiceDate", to_date(col("InvoiceDate"), "dd-MM-yyyy H.mm"))
      .where("year(InvoiceDate) == 2010")
      .withColumn("WeekNumber", weekofyear(col("InvoiceDate")))
      .groupBy("Country", "WeekNumber")
      .agg(countDistinct("InvoiceNo").as("NumberoFInvoices"),
           TotalQuantity,
           InvoiceValue)

    summaryWeekDF
      .coalesce(1)
      .write
      .format("parquet")
      .mode("overwrite")
      .save("output")

    summaryWeekDF.sort("Country", "WeekNumber").show()

    logger.info("Stop AggregatingData!!!!")
    spark.stop()
  }

}
