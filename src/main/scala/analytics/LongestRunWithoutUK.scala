package QuantexaAssignment
package analytics

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import model._

/**
 * Analytics object for finding the greatest number of countries a passenger has been in without being in the UK.
 *
 * This object provides a method to analyze flight data and determine, for each passenger,
 * the longest sequence of consecutive countries visited without encountering the UK.
 *
 * Usage example:
 * {{{
 *   val result = LongestRunWithoutUK(flights)
 * }}}
 */
object LongestRunWithoutUK {

  /**
   * Enhanced logic: Collects all 'from' and 'to' countries in order, removes sequential duplicates,
   * splits by 'uk', and finds the longest run of consecutive non-UK countries.
   */
  def apply(flights: Dataset[Flight]): Dataset[CountryCount] = {
    import flights.sparkSession.implicits._
    flights.groupByKey(_.passengerId).mapGroups { case (passengerId, flightsIter) =>
      val flightsSeq = flightsIter.toSeq.sortBy(_.date.getTime)
      // Build the full journey as a list of countries
      val journey = flightsSeq.flatMap(f => List(f.from.trim.toLowerCase, f.to.trim.toLowerCase))
      // Remove sequentially repeated countries
      val deduped = journey.foldLeft(List.empty[String]) { (acc, country) =>
        if (acc.isEmpty || acc.last != country) acc :+ country else acc
      }
      // Split by 'uk' and find the longest segment
      val segments = deduped.mkString(",").split(",uk,").map(_.split(",").filter(_ != "uk"))
      val maxRun = if (segments.isEmpty) 0 else segments.map(_.length).max
      CountryCount(passengerId, maxRun)
    }.orderBy($"countryCount".desc)
  }

}
