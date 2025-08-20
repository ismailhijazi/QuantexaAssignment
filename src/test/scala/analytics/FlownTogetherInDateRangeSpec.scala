package QuantexaAssignment.analytics

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import QuantexaAssignment.model.Flight
import analytics.SparkTestSession

class FlownTogetherInDateRangeSpec extends AnyFlatSpec with Matchers with SparkTestSession {
  "FlownTogetherInDateRange" should "find pairs flown together in date range at least N times" in {
    import spark.implicits._
    val flights = Seq(
      Flight(1, 101, "uk", "us", java.sql.Date.valueOf("2023-01-10")),
      Flight(2, 101, "uk", "us", java.sql.Date.valueOf("2023-01-10")),
      Flight(1, 102, "uk", "us", java.sql.Date.valueOf("2023-02-10")),
      Flight(2, 102, "uk", "us", java.sql.Date.valueOf("2023-02-10")),
      Flight(1, 103, "uk", "us", java.sql.Date.valueOf("2022-12-10")),
      Flight(2, 103, "uk", "us", java.sql.Date.valueOf("2022-12-10")),
      Flight(1, 104, "uk", "fr", java.sql.Date.valueOf("2023-12-15"))
      // Flight(2, 104, "uk", "fr", java.sql.Date.valueOf("2023-12-15"))
    ).toDS()
    val from = java.sql.Date.valueOf("2023-01-01")
    val to = java.sql.Date.valueOf("2023-12-31")
    val result = FlownTogetherInDateRange(flights, 2, from, to).collect()
    result.length shouldBe 1
    result.head.passenger1Id shouldBe 1
    result.head.passenger2Id shouldBe 2
    result.head.numberOfFlightsTogether shouldBe 2
    result.head.from shouldBe from
    result.head.to shouldBe to
  }
}
