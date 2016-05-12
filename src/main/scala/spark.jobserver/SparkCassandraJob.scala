package spark.jobserver

import spark.jobserver.SparkJobBase
import org.apache.spark.sql.cassandra.CassandraSQLContext

trait SparkCassandraJob extends SparkJobBase {
  type C = CassandraSQLContext
}