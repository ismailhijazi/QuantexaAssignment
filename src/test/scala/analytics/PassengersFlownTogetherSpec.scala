package QuantexaAssignment.analytics

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import QuantexaAssignment.model.Flight
import analytics.SparkTestSession

class PassengersFlownTogetherSpec extends AnyFlatSpec with Matchers with SparkTestSession {
  "PassengersFlownTogether" should "find pairs flown together at least N times" in {
    import spark.implicits._
    val flights = Seq(
      Flight(1, 101, "uk", "us", java.sql.Date.valueOf("2023-01-01")),
      Flight(2, 101, "uk", "us", java.sql.Date.valueOf("2023-01-01")),
      Flight(1, 102, "uk", "us", java.sql.Date.valueOf("2023-01-02")),
      Flight(2, 102, "uk", "us", java.sql.Date.valueOf("2023-01-02")),
      Flight(1, 103, "uk", "us", java.sql.Date.valueOf("2023-01-03")),
      Flight(2, 103, "uk", "us", java.sql.Date.valueOf("2023-01-03")),
      Flight(3, 104, "uk", "us", java.sql.Date.valueOf("2023-01-04"))
    ).toDS()
    val result = PassengersFlownTogether(flights, 3).collect()
    result.length shouldBe 1
    result.head.passenger1Id shouldBe 1
    result.head.passenger2Id shouldBe 2
    result.head.numberOfFlightsTogether shouldBe 3
  }
}
