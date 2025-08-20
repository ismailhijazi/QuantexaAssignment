package QuantexaAssignment.model

/**
 * Represents the longest run of flights for a passenger without visiting the UK.
 *
 * @param passengerId Unique identifier for the passenger
 * @param longestRun Length of the longest run (number of distinct countries visited)
 */
case class LongestRun(
  passengerId: Int,
  longestRun: Long
)
