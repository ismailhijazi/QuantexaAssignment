package QuantexaAssignment

import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import scala.util.{Try, Success, Failure}
import java.nio.file.{Files, Paths}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.sql.Date
import QuantexaAssignment.model.{Flight, Passenger}
import QuantexaAssignment.analytics.{FlownTogetherInDateRange, LongestRunWithoutUK, MostFrequentFlyers, PassengersFlownTogether, TotalFlightsPerMonth}
import QuantexaAssignment.util.CsvUtils
import QuantexaAssignment.util.DataValidationUtils

object Main {
  /**
   * Helper to run analytics with metrics logging.
   * @param label Name of the analytics function for logging
   * @param logger Logger instance
   * @param analyticsFunc Function that returns a Dataset
   * @return The result Dataset
   */
  private def runWithMetrics[T](label: String, logger: Logger)(analyticsFunc: => org.apache.spark.sql.Dataset[T]): org.apache.spark.sql.Dataset[T] = {
    val startTime = System.currentTimeMillis()
    val result = analyticsFunc
    val recordCount = result.count()
    val duration = System.currentTimeMillis() - startTime
    logger.info(s"$label: result count = $recordCount, execution time = ${duration}ms")
    result
  }

  /**
   * Main entry point for the analytics application.
   *
   * @param args Command-line arguments. Usage: run <FunctionName> [params] [--show]
   *             FunctionName: Name of the analytics function to run (e.g., MostFrequentFlyers).
   *             params: Optional parameters for the function (e.g., topN for MostFrequentFlyers).
   *             --show: If present, displays the result in the console.
   *
   * Loads configuration from application.conf, initializes Spark, reads input data, and runs the selected analytics function.
   * Handles errors in configuration, data loading, and analytics execution, logging messages for troubleshooting.
   */
  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(getClass.getName)
    val configTry = Try(ConfigFactory.load())
    configTry match {
      case Failure(e) =>
        logger.error(s"Failed to load configuration: ${e.getMessage}. Please check your application.conf file.")
      case Success(config) =>
        val flightsPath: String = Try(config.getString("quantexa.input.flights")).getOrElse("")
        if (flightsPath.isEmpty) logger.error("Missing 'quantexa.input.flights' in configuration.")
        val passengersPath: String = Try(config.getString("quantexa.input.passengers")).getOrElse("")
        if (passengersPath.isEmpty) logger.error("Missing 'quantexa.input.passengers' in configuration.")
        val sparkMaster = Try(config.getString("quantexa.spark.master")).getOrElse("local[*]")
        val sparkAppName = Try(config.getString("quantexa.spark.appName")).getOrElse("Quantexa Flight Stats Functional")

        val sparkTry = Try(SparkSession.builder().appName(sparkAppName).master(sparkMaster).getOrCreate())
        sparkTry match {
          case Failure(e) =>
            logger.error(s"Failed to create SparkSession: ${e.getMessage}. Please check your Spark configuration.")
          case Success(spark) =>
            import spark.implicits._
            val flightsTry = CsvUtils.safeReadCsv[Flight](spark, flightsPath, Encoders.product[Flight].schema)(Encoders.product[Flight])
            val passengersTry = CsvUtils.safeReadCsv[Passenger](spark, passengersPath, Encoders.product[Passenger].schema)(Encoders.product[Passenger])

            if (args.isEmpty) {
              logger.error("No analytics function specified. Usage: run <FunctionName> [params]")
              spark.stop()
            }
            val functionName = args(0)
            val params = args.drop(1)
            val showResult = params.contains("--show")

            (flightsTry, passengersTry) match {
              case (Success(flights), Success(passengers)) =>
                try {
                  functionName match {
                    case "TotalFlightsPerMonth" =>
                      val result = runWithMetrics("TotalFlightsPerMonth", logger) {
                        TotalFlightsPerMonth(flights)
                      }
                      if (showResult) result.show(false)
                      val outPath = if (params.length > 1 && params(1) != "--show") params(1) else "results/TotalFlightsPerMonth.csv"
                      Try(CsvUtils.saveAsCsv(result, outPath, logger)).recover {
                        case e => logger.error(s"Failed to save results to $outPath: ${e.getMessage}")
                      }
                    case "MostFrequentFlyers" =>
                      // Helper to check if a string is a valid integer (including negative numbers)
                      def isValidInt(s: String): Boolean = s.matches("-?\\d+")
                      val topN = params.find(p => p != "--show" && isValidInt(p)).map(_.toInt).getOrElse(100)
                      val outPath = params.find(p => p != "--show" && !isValidInt(p)).getOrElse("results/MostFrequentFlyers.csv")
                      val result = runWithMetrics("MostFrequentFlyers", logger) {
                        MostFrequentFlyers(flights, passengers, topN)
                      }
                      if (showResult) result.show(false)
                      Try(CsvUtils.saveAsCsv(result, outPath, logger)).recover {
                        case e => logger.error(s"Failed to save results to $outPath: ${e.getMessage}")
                      }
                    case "LongestRunWithoutUK" =>
                      val result = runWithMetrics("LongestRunWithoutUK", logger) {
                        LongestRunWithoutUK(flights)
                      }
                      if (showResult) result.show(false)
                      val outPath = if (params.length > 1 && params(1) != "--show") params(1) else "results/LongestRunWithoutUK.csv"
                      Try(CsvUtils.saveAsCsv(result, outPath, logger)).recover {
                        case e => logger.error(s"Failed to save results to $outPath: ${e.getMessage}")
                      }
                    case "PassengersFlownTogether" =>
                      def isValidInt(s: String): Boolean = s.matches("-?\\d+")
                      val minFlights = params.find(p => p != "--show" && isValidInt(p)).map(_.toInt).getOrElse(3)
                      val outPath = params.find(p => p != "--show" && !isValidInt(p)).getOrElse("results/PassengersFlownTogether.csv")
                      val result = runWithMetrics("PassengersFlownTogether", logger) {
                        PassengersFlownTogether(flights, minFlights)
                      }
                      if (showResult) result.show(false)
                      Try(CsvUtils.saveAsCsv(result, outPath, logger)).recover {
                        case e => logger.error(s"Failed to save results to $outPath: ${e.getMessage}")
                      }
                    case "FlownTogetherInDateRange" =>
                      if (params.length < 3) {
                        logger.error("FlownTogether in Date Range requires 3 parameters: atLeastNTimes, from (yyyy-MM-dd), to (yyyy-MM-dd)")
                      } else {
                        val atLeastNTimes = Try(params(0).toInt).getOrElse({
                          logger.error(s"Invalid atLeastNTimes parameter: ${params(0)}"); return
                        })
                        val from = Try(java.sql.Date.valueOf(params(1))).getOrElse({
                          logger.error(s"Invalid from date parameter: ${params(1)}. Expected format: yyyy-MM-dd"); return
                        })
                        val to = Try(java.sql.Date.valueOf(params(2))).getOrElse({
                          logger.error(s"Invalid to date parameter: ${params(2)}. Expected format: yyyy-MM-dd"); return
                        })
                        val result = runWithMetrics("FlownTogether in Date Range", logger) {
                          FlownTogetherInDateRange(flights, atLeastNTimes, from, to)
                        }
                        if (showResult) result.show(false)
                        val outPath = if (params.length > 3 && params(3) != "--show") params(3) else "results/FlownTogetherInDateRange.csv"
                        Try(CsvUtils.saveAsCsv(result, outPath, logger)).recover {
                          case e => logger.error(s"Failed to save results to $outPath: ${e.getMessage}")
                        }
                      }
                    case _ =>
                      logger.error(s"Unknown function: $functionName. Please check the function name and try again.")
                  }
                } catch {
                  case e: Exception =>
                    logger.error(s"Error during analytics execution: ${e.getMessage}")
                }
              case (Failure(e1), _) => logger.error(s"Failed to load flights from $flightsPath: ${e1.getMessage}")
              case (_, Failure(e2)) => logger.error(s"Failed to load passengers from $passengersPath: ${e2.getMessage}")
            }
            spark.stop()
        }
    }
  }
}
