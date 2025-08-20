package QuantexaAssignment.util

import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset

/**
 * Utility object for validating data quality in Spark Datasets.
 *
 * Provides methods for schema validation, non-empty checks, and extensible data quality rules
 * such as duplicate detection and value range enforcement.
 *
 * Usage example:
 * {{{
 *   val valid = DataValidationUtils.validateSchema(ds, expectedColumns, logger, "Flights")
 * }}}
 */
object DataValidationUtils {
  def validateSchema(ds: Dataset[_], expectedColumns: Seq[String], logger: Logger, name: String): Boolean = {
    val valid = expectedColumns.forall(ds.columns.contains)
    if (!valid) {
      logger.error(s"$name CSV schema invalid. Expected columns: ${expectedColumns.mkString(", ")}. Found: ${ds.columns.mkString(", ")}. Please check your CSV header and ensure it matches the expected format.")
    }
    valid
  }

  def validateNonEmpty(ds: Dataset[_], logger: Logger, name: String): Boolean = {
    val count = ds.count()
    if (count == 0) {
      logger.error(s"$name CSV is empty. Please verify the data source and file path.")
      false
    } else true
  }

  trait DataQualityRule {
    def validate(df: org.apache.spark.sql.DataFrame, logger: Logger, path: String): Unit
  }

  case class DuplicateRule(keys: Seq[String]) extends DataQualityRule {
    def validate(df: org.apache.spark.sql.DataFrame, logger: Logger, path: String): Unit = {
      val duplicateCount = df.groupBy(keys.map(df(_)): _*).count().filter("count > 1").count()
      if (duplicateCount > 0) {
        logger.error(s"Found $duplicateCount duplicate rows in $path for keys: ${keys.mkString(",")}. Please remove or correct duplicate rows in your data.")
        throw new IllegalArgumentException(s"Duplicate rows detected: $duplicateCount for keys: ${keys.mkString(",")}")
      }
    }
  }

  case class OutOfRangeRule(col: String, min: Long, max: Long) extends DataQualityRule {
    def validate(df: org.apache.spark.sql.DataFrame, logger: Logger, path: String): Unit = {
      val outOfRangeCount = df.filter(df(col) < min || df(col) > max).count()
      if (outOfRangeCount > 0) {
        logger.error(s"Found $outOfRangeCount out-of-range values in column '$col' in $path. Expected range: [$min, $max]. Please check your data source for incorrect values.")
        throw new IllegalArgumentException(s"Out-of-range values detected: $outOfRangeCount in column '$col'")
      }
    }
  }

  case class NullCheckRule(cols: Seq[String]) extends DataQualityRule {
    def validate(df: org.apache.spark.sql.DataFrame, logger: Logger, path: String): Unit = {
      val nullCounts = cols.map { colName =>
        val count = df.filter(df(colName).isNull).count()
        (colName, count)
      }.filter(_._2 > 0)
      if (nullCounts.nonEmpty) {
        logger.error(s"Null values found in required columns: " + nullCounts.map { case (c, n) => s"$c: $n" }.mkString(", "))
        throw new IllegalArgumentException(s"Nulls in required columns: " + nullCounts.map { case (c, n) => s"$c: $n" }.mkString(", "))
      }
    }
  }

  case class UniqueRule(col: String) extends DataQualityRule {
    def validate(df: org.apache.spark.sql.DataFrame, logger: Logger, path: String): Unit = {
      val totalCount = df.count()
      val uniqueCount = df.select(col).distinct().count()
      if (uniqueCount < totalCount) {
        logger.error(s"Found ${totalCount - uniqueCount} duplicate $col(s) in $path")
        throw new IllegalArgumentException(s"Duplicate $col(s) detected: ${totalCount - uniqueCount}")
      }
    }
  }

  case class DateYearRangeRule(col: String, minYear: Int, maxYear: Int) extends DataQualityRule {
    def validate(df: org.apache.spark.sql.DataFrame, logger: Logger, path: String): Unit = {
      import org.apache.spark.sql.functions.year
      val outOfRangeCount = df.filter(year(df(col)) < minYear || year(df(col)) > maxYear).count()
      if (outOfRangeCount > 0) {
        logger.error(s"Found $outOfRangeCount out-of-range years in $path for $col (year not in $minYear-$maxYear)")
        throw new IllegalArgumentException(s"Out-of-range years detected: $outOfRangeCount")
      }
    }
  }
}
