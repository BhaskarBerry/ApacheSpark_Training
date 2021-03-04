package berry.training.com.DataSet
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

case class SurveyRecord(Age: Int,
                        Gender: String,
                        Country: String,
                        State: String)

object HelloDataSet extends Serializable {
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
    val rowDF: Dataset[Row] = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(0))

    val surveyDS: Dataset[SurveyRecord] =
      rowDF.select("Age", "Gender", "Country", "State").as[SurveyRecord]

    val filterDS = surveyDS.filter(row => row.Age < 40)
    val filterDF = surveyDS.filter("Age < 40")

    //Type safe group by
    val countDS = filterDS.groupByKey(r => r.Country).count()

    //Runtime group by
    val countDF = filterDF.groupBy("Country").count()

    logger.info("Dataset: " + countDS.collect().mkString(","))
    logger.info("DataFrame: " + countDF.collect().mkString(","))
    logger.info("Stop Hello Spark!!!!")

    spark.stop()
  }

}
