package QuantexaAssignment.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import QuantexaAssignment.model.Flight

class CsvUtilsSpec extends AnyFlatSpec with Matchers {
  val spark = SparkSession.builder().master("local[*]").appName("CsvUtilsTest").getOrCreate()
  import spark.implicits._
  import java.nio.file.Files

  "safeReadCsv" should "read a valid CSV file and return a Dataset" in {
    val flights = Seq(
      Flight(1, 101, "uk", "us", java.sql.Date.valueOf("2022-01-01")),
      Flight(2, 102, "us", "uk", java.sql.Date.valueOf("2022-01-02"))
    ).toDS()
    val tempDir = Files.createTempDirectory("test_flights_csv").toFile
    val csvPath = tempDir.getAbsolutePath + "/test_flights.csv"
    flights.write.option("header", "true").csv(csvPath)
    val dsTry = CsvUtils.safeReadCsv[Flight](spark, csvPath, Encoders.product[Flight].schema)(Encoders.product[Flight])
    dsTry.isSuccess shouldBe true
    dsTry.get.count() shouldBe 2
    tempDir.delete()
  }

  "saveAsCsv" should "save a Dataset to CSV and return true" in {
    val flights = Seq(
      Flight(1, 101, "uk", "us", java.sql.Date.valueOf("2022-01-01")),
      Flight(2, 102, "us", "uk", java.sql.Date.valueOf("2022-01-02"))
    ).toDS()
    val tempDir = Files.createTempDirectory("test_flights_out_csv").toFile
    val outPath = tempDir.getAbsolutePath + "/test_flights_out.csv"
    val logger = org.apache.log4j.Logger.getLogger(getClass.getName)
    val result = CsvUtils.saveAsCsv(flights, outPath, logger)
    result shouldBe true
    tempDir.delete()
  }
}
