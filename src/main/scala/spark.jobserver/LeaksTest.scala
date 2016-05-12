package leaks

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._
import scala.util.Try

object LeaksTest extends spark.jobserver.SparkJob {
  override def runJob(sc: SparkContext, config: Config): Any = {
    sc.parallelize(config.getString("string").split("").toSeq).countByValue
  }

  override def validate(sc: SparkContext, config: Config): spark.jobserver.SparkJobValidation = {
    Try(config.getString("string"))
      .map(x => spark.jobserver.SparkJobValid)
      .getOrElse(spark.jobserver.SparkJobInvalid("No 'string' given"))
  }
  // sc.stop
  // val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")
  // val sc = new SparkContext(conf)
  //print("hej")
}