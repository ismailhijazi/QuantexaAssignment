package QuantexaAssignment
package util

import org.apache.spark.sql.{SparkSession, Dataset, SaveMode}
import org.apache.spark.sql.types.StructType
import com.typesafe.config.ConfigFactory
import scala.util.{Try, Success, Failure}

/**
 * Enhancement 7: Utility for Parquet operations - provides 5-10x faster reads than CSV
 *
 * Parquet is a columnar storage format that provides:
 * - Much faster read performance (5-10x faster than CSV)
 * - Better compression (typically 50-80% smaller files)
 * - Schema evolution support
 * - Predicate pushdown for efficient filtering
 */
object ParquetUtils {

  /**
   * Converts CSV data to Parquet format for better performance.
   * Use this once to convert your data, then update config to read from Parquet files.
   */
  def convertCsvToParquet(spark: SparkSession, csvPath: String, parquetPath: String, schema: StructType): Try[Unit] = {
    val logger = org.apache.log4j.Logger.getLogger(getClass.getName)

    Try {
      logger.info(s"Converting CSV from $csvPath to Parquet at $parquetPath")

      val df = spark.read
        .option("header", "true")
        .schema(schema)
        .csv(csvPath)

      df.write
        .mode(SaveMode.Overwrite)
        .option("compression", "snappy")  // Fast compression
        .parquet(parquetPath)

      logger.info(s"Successfully converted to Parquet format at $parquetPath")
    }
  }

  /**
   * Reads data from Parquet format - much faster than CSV.
   */
  def readParquet[T](spark: SparkSession, path: String)(implicit encoder: org.apache.spark.sql.Encoder[T]): Try[Dataset[T]] = {
    val logger = org.apache.log4j.Logger.getLogger(getClass.getName)

    Try {
      logger.info(s"Reading Parquet data from $path")
      spark.read.parquet(path).as[T]
    }
  }

  /**
   * Saves Dataset to Parquet format with compression.
   */
  def saveAsParquet[T](ds: Dataset[T], path: String, logger: org.apache.log4j.Logger): Boolean = {
    try {
      ds.write
        .mode(SaveMode.Overwrite)
        .option("compression", "snappy")
        .parquet(path)

      logger.info(s"Saved Parquet data to $path with snappy compression")
      true
    } catch {
      case e: Exception =>
        logger.error(s"Failed to save Parquet data to $path: ${e.getMessage}")
        false
    }
  }
}
