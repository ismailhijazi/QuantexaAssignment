package QuantexaAssignment
package model

/**
 * Represents a flight taken by a passenger.
 *
 * @param passengerId Unique identifier for the passenger
 * @param flightId Unique identifier for the flight
 * @param from Departure location
 * @param to Arrival location
 * @param date Date of the flight
 */
case class Flight(
  passengerId: Int,
  flightId: Int,
  from: String,
  to: String,
  date: java.sql.Date
)
