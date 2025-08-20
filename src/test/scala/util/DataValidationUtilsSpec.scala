package QuantexaAssignment.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import QuantexaAssignment.model.Flight

class DataValidationUtilsSpec extends AnyFlatSpec with Matchers {
  val spark = SparkSession.builder().master("local[*]").appName("DataValidationUtilsTest").getOrCreate()
  import spark.implicits._
  val logger = org.apache.log4j.Logger.getLogger(getClass.getName)

  "validateSchema" should "return true for valid schema" in {
    val flights = Seq(
      Flight(1, 101, "uk", "us", java.sql.Date.valueOf("2022-01-01"))
    ).toDS()
    val expectedColumns = Seq("passengerId", "flightId", "from", "to", "date")
    DataValidationUtils.validateSchema(flights, expectedColumns, logger, "Flights") shouldBe true
  }

  it should "return false for invalid schema" in {
    val flights = Seq(
      Flight(1, 101, "uk", "us", java.sql.Date.valueOf("2017-01-01"))
    ).toDF().drop("date")
    val expectedColumns = Seq("passengerId", "flightId", "from", "to", "date")
    DataValidationUtils.validateSchema(flights, expectedColumns, logger, "Flights") shouldBe false
  }

  "validateNonEmpty" should "return true for non-empty dataset" in {
    val flights = Seq(
      Flight(1, 101, "uk", "us", java.sql.Date.valueOf("2017-01-01"))
    ).toDS()
    DataValidationUtils.validateNonEmpty(flights, logger, "Flights") shouldBe true
  }

  it should "return false for empty dataset" in {
    val flights = spark.emptyDataset[Flight]
    DataValidationUtils.validateNonEmpty(flights, logger, "Flights") shouldBe false
  }
}
