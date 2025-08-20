package QuantexaAssignment.analytics

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import QuantexaAssignment.model.Flight
import analytics.SparkTestSession

class TotalFlightsPerMonthSpec extends AnyFlatSpec with Matchers with SparkTestSession {
  "TotalFlightsPerMonth" should "count flights per month" in {
    import spark.implicits._
    val flights = Seq(
      Flight(1, 101, "cg", "ir", java.sql.Date.valueOf("2017-01-01")),
      Flight(2, 102, "cg", "ir", java.sql.Date.valueOf("2017-01-01")),
      Flight(3, 103, "cg", "ir", java.sql.Date.valueOf("2017-02-01")),
      Flight(4, 104, "cg", "ir", java.sql.Date.valueOf("2017-02-10"))
    ).toDS()
    val result = TotalFlightsPerMonth(flights).collect()
    result.length shouldBe 2
    val jan = result.find(_.month == 1).get
    val feb = result.find(_.month == 2).get
    jan.numberOfFlights shouldBe 2
    feb.numberOfFlights shouldBe 2
  }
}
