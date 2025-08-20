package analytics

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession
import QuantexaAssignment.model._
import QuantexaAssignment.analytics.LongestRunWithoutUK
import java.sql.Date

class LongestRunWithoutUKSpec extends AnyFlatSpec with Matchers {
  val spark = SparkSession.builder().master("local[*]").appName("Test").getOrCreate()
  import spark.implicits._

  def flight(pid: Int, fid: Int, from: String, to: String, date: String): Flight =
    Flight(pid, fid, from, to, Date.valueOf(date))

  "LongestRunWithoutUK" should "handle passenger never visits UK" in {
    val flights = Seq(
      flight(1, 0, "FR", "US", "2017-01-01"),
      flight(1, 1, "US", "CN", "2017-01-02"),
      flight(1, 2, "CN", "DE", "2017-01-03")
    ).toDS
    val result = LongestRunWithoutUK(flights).collect()
    result should contain (CountryCount(1, 4))
  }

  it should "handle passenger starts in UK" in {
    val flights = Seq(
      flight(2, 0, "UK", "FR", "2017-01-01"),
      flight(2, 1, "FR", "US", "2017-01-02"),
      flight(2, 2, "US", "CN", "2017-01-03")
    ).toDS
    val result = LongestRunWithoutUK(flights).collect()
    result should contain (CountryCount(2, 3))
  }

  it should "handle multiple consecutive UK visits" in {
    val flights = Seq(
      flight(3, 0, "UK", "FR", "2017-01-01"),
      flight(3, 1, "FR", "UK", "2017-01-02"),
      flight(3, 2, "UK", "US", "2017-01-03"),
      flight(3, 3, "US", "CN", "2017-01-04"),
      flight(3, 4, "CN", "DE", "2017-01-05"),
      flight(3, 5, "DE", "UK", "2017-01-06")
    ).toDS
    val result = LongestRunWithoutUK(flights).collect()
    result should contain (CountryCount(3, 3))
  }

  it should "handle repeated countries in sequence" in {
    val flights = Seq(
      flight(4, 0, "FR", "FR", "2017-01-01"),
      flight(4, 1, "FR", "FR", "2017-01-02"),
      flight(4, 2, "FR", "DE", "2017-01-03"),
      flight(4, 3, "DE", "DE", "2017-01-04")
    ).toDS
    val result = LongestRunWithoutUK(flights).collect()
    result should contain (CountryCount(4, 2)) // FR, DE are counted after deduplication
  }

  it should "handle passenger starts in UK then flights in the same country multiple times" in {
    val flights = Seq(
      flight(5, 0, "UK", "FR", "2017-01-01"),
      flight(5, 1, "FR", "FR", "2017-01-02"),
      flight(5, 2, "FR", "FR", "2017-01-03")
    ).toDS
    val result = LongestRunWithoutUK(flights).collect()
    result should contain (CountryCount(5, 1))
  }

  it should "handle passenger with route uk → th → th → th (count 1)" in {
    val flights = Seq(
      flight(7232, 0, "UK", "TH", "2017-01-01"),
      flight(7232, 1, "TH", "TH", "2017-01-02"),
      flight(7232, 2, "TH", "TH", "2017-01-03")
    ).toDS
    val result = LongestRunWithoutUK(flights).collect()
    result should contain (CountryCount(7232, 1))
  }

  it should "handle passenger with route ar → uk → uk → uk (count 1)" in {
    val flights = Seq(
      flight(9591, 0, "AR", "UK", "2017-01-01"),
      flight(9591, 1, "UK", "UK", "2017-01-02"),
      flight(9591, 2, "UK", "UK", "2017-01-03")
    ).toDS
    val result = LongestRunWithoutUK(flights).collect()
    result should contain (CountryCount(9591, 1))
  }

  it should "handle passenger with route cg → tj → tj → tj → uk → se → th (count 2)" in {
    val flights = Seq(
      flight(10001, 0, "CG", "TJ", "2017-01-01"),
      flight(10001, 1, "TJ", "TJ", "2017-01-02"),
      flight(10001, 2, "TJ", "UK", "2017-01-03"),
      flight(10001, 3, "UK", "SE", "2017-01-04"),
      flight(10001, 4, "SE", "TH", "2017-01-05")
    ).toDS
    val result = LongestRunWithoutUK(flights).collect()
    result should contain (CountryCount(10001, 2))
  }
}
