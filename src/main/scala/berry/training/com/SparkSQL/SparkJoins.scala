package berry.training.com.SparkSQL

import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkJoins extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL Joins Demo")
      .master("local[3]")
      .getOrCreate()

    val ordersList = List(
      ("01", "02", 350, 1),
      ("01", "04", 580, 1),
      ("01", "07", 320, 2),
      ("02", "03", 450, 1),
      ("02", "06", 220, 1),
      ("03", "01", 195, 1),
      ("04", "09", 270, 3),
      ("04", "08", 410, 2),
      ("05", "02", 350, 1)
    )

    val orderDF = spark
      .createDataFrame(ordersList)
      .toDF("order_id", "prod_id", "unit_price", "qty")
    orderDF.show(15)

    val productList = List(
      ("01", "Scroll Mouse", 250, 20),
      ("02", "Optical Mouse", 250, 20),
      ("03", "Wireless Mouse", 250, 20),
      ("04", "Wireless KB", 250, 20),
      ("05", "Standard KB", 250, 20),
      ("06", "16 GB Flash Storage", 240, 100),
      ("07", "32 GB Flash Storage", 320, 50),
      ("08", "64 GB Flash Storage", 430, 25)
    )

    val productDF = spark
      .createDataFrame(productList)
      .toDF("prod_id", "prod_name", "list_price", "qty")
    productDF.show(20)

    val productRenamedDF = productDF.withColumnRenamed("qty", "reorder_qty")

    val joinExpr = orderDF.col("prod_id") === productDF.col("prod_id")
    val joinType = "left"
    /*
     inner
     outer
     Outer join - Full Outer
     Left Join - Left Outer
     Right join - Right Outer

     */

    import org.apache.spark.sql.functions._
    orderDF
      .join(productRenamedDF, joinExpr, joinType)
      .drop(productRenamedDF.col("prod_id"))
      .select("order_id",
        "prod_id",
        "prod_name",
        "unit_price",
        "list_price",
        "qty")
      .withColumn("prod_name", expr("coalesce(prod_name, prod_id)"))
      .withColumn("list_price", expr("coalesce(list_price, unit_price)"))
      .sort("order_id")
      .show()

    logger.info("Finished.")
    spark.stop()
  }
}
