package QuantexaAssignment.model

/**
 * Represents a pair of passengers who have flown together and the number of flights shared.
 *
 * @param passenger1Id Unique identifier for the first passenger
 * @param passenger2Id Unique identifier for the second passenger
 * @param numberOfFlightsTogether Number of flights the pair has shared
 */
case class FlownTogether(
  passenger1Id: Int,
  passenger2Id: Int,
  numberOfFlightsTogether: Long
)
