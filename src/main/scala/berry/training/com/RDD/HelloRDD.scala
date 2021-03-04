package berry.training.com.RDD

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

case class SurveyRecord(Age: Int, Gender: String, Country: String, State:String)

object HelloRDD extends Serializable{
  def main(args: Array[String]): Unit = {
    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

    if(args.length == 0){
      logger.info("HelloRDD needs filename to be read!!!")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("HelloRDD").setMaster("local[3]")

    val sparkContext = new SparkContext(sparkConf)

    val linesRDD = sparkContext.textFile(args(0))

    val partitionedRDD = linesRDD.repartition(2)

    val colRDD = partitionedRDD.map(line => line.split(",").map(_.trim))

    val selectRDD = colRDD.map(cols => SurveyRecord(cols(1).toInt, cols(2),cols(3),cols(4)))

    val filteredRDD = selectRDD.filter(row => row.Age < 40)

    val kvRDD = filteredRDD.map(row => (row.Country, 1))

    val countRDD = kvRDD.reduceByKey((v1, v2) => v1+ v2)

    logger.info(countRDD.collect().mkString(","))

    sparkContext.stop()
  }
}
