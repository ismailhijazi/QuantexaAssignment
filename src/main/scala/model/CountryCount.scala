package QuantexaAssignment.model

/**
 * Represents the number of unique countries visited by a passenger before visiting the UK.
 *
 * @param passengerId Unique identifier for the passenger
 * @param countryCount Number of unique countries visited before first UK visit
 */
case class CountryCount(
  passengerId: Int,
  countryCount: Long
)

