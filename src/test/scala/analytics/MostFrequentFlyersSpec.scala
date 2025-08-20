package QuantexaAssignment
package analytics

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession
import model.{Flight, Passenger}

class MostFrequentFlyersSpec extends AnyFlatSpec with Matchers {
  val spark: SparkSession = SparkSession.builder().master("local[*]").appName("MostFrequentFlyersTest").getOrCreate()
  import spark.implicits._

  "MostFrequentFlyers" should "return the top N frequent flyers" in {
    val flights = Seq(
      Flight(1, 101, "uk", "us", java.sql.Date.valueOf("2022-01-01")),
      Flight(1, 102, "uk", "us", java.sql.Date.valueOf("2022-01-02")),
      Flight(2, 103, "us", "uk", java.sql.Date.valueOf("2022-01-03"))
    ).toDS()
    val passengers = Seq(
      Passenger(1, "John", "Doe"),
      Passenger(2, "Jane", "Smith")
    ).toDS()
    val result = MostFrequentFlyers(flights, passengers, topN = 1).collect()
    result.length shouldBe 1
    result.head.passengerId shouldBe 1
    result.head.numberOfFlights shouldBe 2
    result.head.firstName shouldBe "John"
    result.head.lastName shouldBe "Doe"
  }
}
