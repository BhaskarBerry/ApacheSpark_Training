package berry.training.com.UDF

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object UDFDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      logger.error("There is no filename")
      System.exit(1)
    }

    // Create Spark Session
    val spark = SparkSession
      .builder()
      .appName("UDF Demo")
      .master("local[3]")
      .getOrCreate()

    // Read your CSV file - survey.csv
    val surveyDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(0))

    surveyDF.show()

    import org.apache.spark.sql.functions._

    val parseGenderUDF = udf(parseGender(_:String): String)
    spark.catalog.listFunctions().filter(r=>r.name == "parseGenderUDF").show()

    val surveyDF2 = surveyDF.withColumn("Gender", parseGenderUDF(col("Gender")))

    surveyDF2.show()

    spark.udf.register("parseGenderUDF", parseGender(_:String): String)
    spark.catalog.listFunctions().filter(r=>r.name == "parseGenderUDF").show()
    val surveyDF3 = surveyDF.withColumn("Gender", expr("parseGenderUDF(Gender)"))

    surveyDF3.show()

    logger.info("Stop UDFDemo!!!!")
    spark.stop()
  }

  def parseGender(s: String): String = {
    val femalePattern = "^f$|f.m|w.m".r
    val malePattern = "^m$|ma|m.l".r

    if(femalePattern.findFirstIn(s.toLowerCase).nonEmpty) "Female"
    else if (malePattern.findFirstIn(s.toLowerCase).nonEmpty) "Male"
    else "Unknown"
  }
}
