package spark.jobserver.context

import com.typesafe.config.Config
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import spark.jobserver.{ContextLike, SparkCassandraJob, SparkJobBase}
import spark.jobserver.util.SparkJobUtils
import org.apache.spark.sql.cassandra.CassandraSQLContext

class CassandraContextFactory extends SparkContextFactory {
  type C = CassandraSQLContext with ContextLike

  def makeContext(sparkConf: SparkConf, config: Config,  contextName: String): C = {
    new CassandraSQLContext(new SparkContext(sparkConf)) with ContextLike {
      def isValidJob(job: SparkJobBase): Boolean = job.isInstanceOf[SparkCassandraJob]
      def stop() { this.sparkContext.stop() }
    }
  }
}