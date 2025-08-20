package QuantexaAssignment.model

/**
 * Represents the total number of flights for a given month.
 *
 * @param month Month number (1-12)
 * @param numberOfFlights Number of flights in the month
 */
case class MonthlyFlightCount(
  month: Int,
  numberOfFlights: Long
)
