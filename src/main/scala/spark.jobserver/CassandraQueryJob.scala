package spark.jobserver

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._
import spark.jobserver._
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra.CassandraSQLContext
import scala.util.Try

object CassandraQuery extends SparkCassandraJob {

  override def runJob(cc: CassandraSQLContext, config: Config): Any = {
    // Assuming your request sent a { "query": "..." } in the body:
    val df = cc.sql(config.getString("query"))
    df.collect()
    // createResponseFromDataFrame(df) // You should implement this
  }
  override def validate(cc: CassandraSQLContext, config: Config): SparkJobValidation = {
    // Code to validate the parameters received in the request body
    // Try(config.getString("query"))
    //  .map(x => SparkJobValid)
    //  .getOrElse(SparkJobInvalid("No 'query' given"))
    SparkJobValid
  }
}