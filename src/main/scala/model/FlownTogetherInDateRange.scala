package QuantexaAssignment.model

/**
 * Represents a pair of passengers who have flown together within a specific date range.
 *
 * @param passenger1Id Unique identifier for the first passenger
 * @param passenger2Id Unique identifier for the second passenger
 * @param numberOfFlightsTogether Number of flights the pair has shared
 * @param from Start date of the range (inclusive)
 * @param to End date of the range (inclusive)
 */
case class FlownTogetherInDateRange(
  passenger1Id: Int,
  passenger2Id: Int,
  numberOfFlightsTogether: Long,
  from: java.sql.Date,
  to: java.sql.Date
)
