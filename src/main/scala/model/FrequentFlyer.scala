package QuantexaAssignment.model

/**
 * Represents a frequent flyer and their flight count, including name details.
 *
 * @param passengerId Unique identifier for the passenger
 * @param numberOfFlights Number of flights taken by the passenger
 * @param firstName Passenger's first name
 * @param lastName Passenger's last name
 */
case class FrequentFlyer(
  passengerId: Int,
  numberOfFlights: Long,
  firstName: String,
  lastName: String
)
