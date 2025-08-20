package analytics

import org.apache.spark.sql.SparkSession

trait SparkTestSession {
  lazy val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("AnalyticsTestSession")
    .getOrCreate()
}

