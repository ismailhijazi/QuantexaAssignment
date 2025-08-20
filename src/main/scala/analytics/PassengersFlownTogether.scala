package QuantexaAssignment.analytics

import org.apache.spark.sql.Dataset
import QuantexaAssignment.model._
import org.apache.spark.sql.functions._

/**
 * Analytics object for finding pairs of passengers who have flown together at least a minimum number of times.
 *
 * This object provides a method to analyze flight data and identify pairs of passengers
 * who have shared flights together at least a specified number of times.
 *
 * Usage example:
 * {{{
 *   val result = PassengersFlownTogether(flights, 3)
 * }}}
 */
object PassengersFlownTogether {
  /**
   * Finds pairs of passengers who have flown together at least a minimum number of times.
   * @param flights Dataset of Flight records
   * @param minFlights Minimum number of flights together (default 3)
   * @return Dataset of FlownTogether records
   */
  def apply(flights: Dataset[Flight], minFlights: Int = 3): Dataset[FlownTogether] = {
    import flights.sparkSession.implicits._

    // New optimized implementation using DataFrame API
    val pairs = flights
      .as("f1")
      .join(flights.as("f2"), ($"f1.flightId" === $"f2.flightId") && ($"f1.passengerId" < $"f2.passengerId"))
      .select($"f1.passengerId".as("passenger1Id"), $"f2.passengerId".as("passenger2Id"))
      .groupBy("passenger1Id", "passenger2Id")
      .agg(count(lit(1)).as("numberOfFlightsTogether"))
      .filter($"numberOfFlightsTogether" >= minFlights)
      .orderBy($"numberOfFlightsTogether".desc)
      .as[FlownTogether]

    pairs
  }
  // old implementation for reference , performance is not optimal
  def applyOld(flights: Dataset[Flight], minFlights: Int = 3): Dataset[FlownTogether] = {
    import flights.sparkSession.implicits._

    // Convert Dataset to RDD
    val flightsRDD = flights.rdd.map(flight => (flight.flightId, flight.passengerId))

    // Perform self-join on flightId
    val joinedRDD = flightsRDD.join(flightsRDD)
      .filter { case (_, (passenger1, passenger2)) => passenger1 < passenger2 } // Ensure unique pairs

    // Map to passenger pairs and count occurrences
    val pairsRDD = joinedRDD
      .map { case (_, (passenger1, passenger2)) => ((passenger1, passenger2), 1) }
      .reduceByKey(_ + _)

    // Filter by minFlights and convert to Dataset
    pairsRDD
      .filter { case (_, count) => count >= minFlights }
      .map { case ((passenger1, passenger2), count) => FlownTogether(passenger1, passenger2, count) }
      .toDS()
      .orderBy($"numberOfFlightsTogether".desc)
  }

}


