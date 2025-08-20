package QuantexaAssignment.analytics

import org.apache.spark.sql.Dataset
import java.sql.Date
import QuantexaAssignment.model._

/**
 * Analytics object for finding pairs of passengers who have flown together at least N times within a date range.
 *
 * This object provides a method to filter flights by date range and then delegates to PassengersFlownTogether
 * to identify pairs of passengers meeting the criteria.
 *
 * Usage example:
 * {{{
 *   val result = FlownTogetherInDateRange(flights, 3, Date.valueOf("2025-01-01"), Date.valueOf("2025-12-31"))
 * }}}
 */
object FlownTogetherInDateRange {

  /**
   * Finds pairs of passengers who have flown together at least N times within a date range.
   * @param flights Dataset of Flight records
   * @param atLeastNTimes Minimum number of times passengers must have flown together
   * @param from Start date (inclusive)
   * @param to End date (inclusive)
   * @return Dataset of FlownTogether records
   */
  def apply(flights: Dataset[Flight], atLeastNTimes: Int, from: Date, to: Date): Dataset[QuantexaAssignment.model.FlownTogetherInDateRange] = {
    import flights.sparkSession.implicits._
    // Filter flights to only those within the specified date range
    val filtered = flights.filter(f => !f.date.before(from) && !f.date.after(to))
    // Delegate to PassengersFlownTogether to find pairs
    val result = PassengersFlownTogether(filtered, atLeastNTimes)
    result.map(ft => QuantexaAssignment.model.FlownTogetherInDateRange(ft.passenger1Id, ft.passenger2Id, ft.numberOfFlightsTogether, from, to))
  }
}
