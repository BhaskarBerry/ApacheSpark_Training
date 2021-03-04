package berry.training.com.DataFrame

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.Properties
import scala.io.Source

object HelloSpark extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Starting Hello Spark!!!!")

    if (args.length == 0) {
      logger.error("There is no filename")
      System.exit(1)
    }

    //    val sparkAppConf = new SparkConf()
    //    sparkAppConf.set("spark.app.name", "Hello")
    //    sparkAppConf.set("spark.master", "local[*]")

    val spark = SparkSession
      .builder()
      .config(getSparkAppConf)
      .getOrCreate()

    logger.info("spark.conf" + spark.conf.getAll.toString())

    val surveyDF = loadCSV(spark, args(0))
    val partitionSurveyDF = surveyDF.repartition(2)
    val countDf = countByCountry(partitionSurveyDF)

    countDf.foreach(
      row =>
        logger.info(
          "Country " + row.getString(0) + " Count " + row.get(1).toString))

    //    logger.info(countDf.collect().mkString("->"))
    surveyDF.show()
    countDf.show()

    logger.info("Stop Hello Spark!!!!")
    //    scala.io.StdIn.readLine()
    spark.stop()
  }

  def getSparkAppConf = {
    val sparkAppConf = new SparkConf()

    val props = new Properties
    props.load(Source.fromFile("spark.conf").bufferedReader())
    props.forEach((k, v) => sparkAppConf.set(k.toString, v.toString))
    sparkAppConf
  }

  def loadCSV(spark: SparkSession, dataFile: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(dataFile)
  }

  def countByCountry(surveyDF: DataFrame): DataFrame = {
    surveyDF
      .where("Age < 40")
      .select("Age", "Gender", "Country", "state")
      .groupBy("Country")
      .count()
  }
}
