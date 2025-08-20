package QuantexaAssignment
package model

/**
 * Represents a passenger in the flight dataset.
 *
 * @param passengerId Unique identifier for the passenger
 * @param firstName Passenger's first name
 * @param lastName Passenger's last name
 */
case class Passenger(
  passengerId: Int,
  firstName: String,
  lastName: String
)
