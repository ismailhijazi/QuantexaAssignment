package QuantexaAssignment
package analytics

import org.apache.spark.sql.Dataset
import model._

/**
 * Analytics object for finding the top N most frequent flyers based on flight count.
 *
 * This object provides a method to aggregate flight data, join with passenger details,
 * and return the top N passengers sorted by number of flights taken.
 *
 * Usage example:
 * {{{
 *   val result = MostFrequentFlyers(flights, passengers, 50)
 * }}}
 */
object MostFrequentFlyers {
  /**
   * Finds the top N most frequent flyers based on flight count.
   *
   * @param flights Dataset of Flight records. Each record must have a valid passengerId.
   * @param passengers Dataset of Passenger records. Each record must have a valid passengerId.
   * @param topN Number of top flyers to return (default 100). Must be positive.
   * @return Dataset[FrequentFlyer]: Each record contains passengerId, numberOfFlights, firstName, lastName. Ordered by numberOfFlights descending.
   *
   * @note Returns an empty Dataset if no flights or passengers are found. Handles duplicate passengerIds by aggregating flight counts.
   */
  def apply(flights: Dataset[Flight], passengers: Dataset[Passenger], topN: Int = 100): Dataset[FrequentFlyer] = {
    import flights.sparkSession.implicits._
    flights
      .groupByKey(_.passengerId: Int)
      .count()
      .joinWith(passengers, $"value" === $"passengerId")
      .map { case ((passengerId, count), passenger) =>
        FrequentFlyer(passengerId, count, passenger.firstName, passenger.lastName)
      }
      .orderBy($"numberOfFlights".desc)
      .limit(topN)
  }
}
