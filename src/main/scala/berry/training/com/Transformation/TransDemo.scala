package berry.training.com.Transformation

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object TransDemo extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    // Create Spark Session
    val spark = SparkSession
      .builder()
      .appName("Misc Demo")
      .master("local[3]")
      .getOrCreate()

    val dataList = List(
      ("Berry", 17, 12, 1990),
      ("Raju", 4, 2, 1995),
      ("Merry", 7, 5, 63),
      ("Mala", 5, 12, 2020),
      ("Raju", 4, 2, 1995),
      ("Ayub", 1, 1, 81)
    )

    val rowDF =
      spark
        .createDataFrame(dataList)
        .toDF("name", "day", "month", "year")
        .repartition(3)
    rowDF.printSchema()

    // 1. way to cast
    val finalDF = rowDF
      .withColumn("id", monotonically_increasing_id)
      .withColumn(
        "year",
        expr("""
               |case when year < 21 then cast(year as int) + 2000
               | when year < 100 then cast(year as int) + 1900
               | else year
               | end
        """.stripMargin)
      )

    //2. way to cast
    val finalDF1 = rowDF
      .withColumn("id", monotonically_increasing_id)
      .withColumn(
        "year",
        expr("""
               |case when year < 21 then year + 2000
               | when year < 100 then year + 1900
               | else year
               | end
        """.stripMargin).cast(IntegerType)
      )

    //3.rd Way
    val finalDF2 = rowDF
      .withColumn("id", monotonically_increasing_id)
      .withColumn("day", col("day").cast(IntegerType))
      .withColumn("month", col("month").cast(IntegerType))
      .withColumn("year", col("year").cast(IntegerType))
      .withColumn(
        "year",
        expr("""
               |case when year < 21 then year + 2000
               | when year < 100 then year + 1900
               | else year
               | end
    """.stripMargin)
      )

    //With column expression
    val finalDF3 = rowDF
      .withColumn("id", monotonically_increasing_id)
      .withColumn("year",
        when(col("year") < 21, col("year") + 2000)
          when (col("year") < 100, col("year") + 1900)
          otherwise (col("year")))
    finalDF3.show()

    // Add and remove duplicates

    val finalDF4 = rowDF
      .withColumn("id", monotonically_increasing_id)
      .withColumn("day", col("day").cast(IntegerType))
      .withColumn("month", col("month").cast(IntegerType))
      .withColumn("year", col("year").cast(IntegerType))
      .withColumn("year",
        when(col("year") < 21, col("year") + 2000)
          when (col("year") < 100, col("year") + 1900)
          otherwise (col("year")))
      .withColumn("dob",
        expr("to_date(concat(day,'/',month,'/',year),'d/M/y')"))
      //      .withColumn("dob", to_date(expr("concat(day,'/',month,'/',year)"),"d/M/y"))
      .drop("day", "month", "year")
      .dropDuplicates("name", "dob")
      //      .sort("dob")
      .sort(col("dob").desc)
    finalDF4.show()

    logger.info("Stop UDFDemo!!!!")
    spark.stop()
  }

}

