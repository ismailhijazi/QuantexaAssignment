# QuantexaAssignment

## Overview
This project solves the Quantexa coding assignment using Spark and Scala in a functional programming style. It processes flight and passenger data to answer four analytics questions and provides results as CSV files.

## Prerequisites
- Java 8+
- Scala 2.12+
- SBT (Scala Build Tool)
- Spark (included as dependency)

## Data Files
Place the following CSV files in the `data/` directory:
- `flightData.csv`
- `passengers.csv`

## How to Run
1. Open a terminal in the project root directory.
2. To run analytics, use:
   ```
   sbt "run <FunctionName> [params] [--show]"
   ```
   - `<FunctionName>`: One of the following:
     - `TotalFlightsPerMonth` (Q1)
     - `MostFrequentFlyers` (Q2)
     - `LongestRunWithoutUK` (Q3)
     - `PassengersFlownTogether` (Q4)
     - `FlownTogetherInDateRange` (Bonus)
   - `[params]`: Optional parameters for some functions (see below).
   - `--show`: Display results in the console.

### Example Commands
- Q1: Total flights per month
  ```
  sbt "run TotalFlightsPerMonth --show"
  ```
- Q2: 100 most frequent flyers
  ```
  sbt "run MostFrequentFlyers --show"
  ```
- Q3: Longest run without UK (ordered by longest run descending)
  ```
  sbt "run LongestRunWithoutUK --show"
  ```
- Q4: Passengers with >3 flights together (ordered by flights together descending)
  ```
  sbt "run PassengersFlownTogether --show"
  ```
- Bonus: Passengers with >N flights together in date range
  ```
  sbt "run FlownTogetherInDateRange <atLeastNTimes> <fromDate> <toDate> --show"
  # Example:
  sbt "run FlownTogetherInDateRange 5 2017-01-01 2017-03-01 --show"
  ```

## Output
Results are saved as CSV files in the `results/` directory:
- `TotalFlightsPerMonth.csv`
- `MostFrequentFlyers.csv`
- `LongestRunWithoutUK.csv`
- `PassengersFlownTogether.csv`
- `FlownTogetherInDateRange.csv` (bonus)

## How to Run Tests
To run unit tests:
```
sbt test
```
Test reports are generated in `target/test-reports/`.

## Testing Strategy
Unit tests are provided for both utility and analytics modules. The tests cover:
- Data validation (schema, non-empty datasets)
- CSV and Parquet reading/writing
- Analytics logic for all assignment questions
- Error handling (invalid schema, empty files, corrupted data)

Tests are written using ScalaTest and can be run with `sbt test`. Test reports are generated in `target/test-reports/` for review. This ensures correctness, robustness, and maintainability of the codebase.

Manual testing was performed using a small sample of flight and passenger data to verify analytics outputs and ensure the application works as expected with real input files and edge cases. Sample data files are included for this purpose.

Integration testing is supported by running the full workflow with real or synthetic data, validating end-to-end correctness from input to output.

## Packaging for Submission
Zip the entire project directory, including:
- Source code
- Output CSV files
- README.md
- Test reports
- ARCHITECTURE.md
- BIG_DATA_ENHANCEMENTS.md
- Data files (e.g., flightData.csv, passengers.csv)

## Functional Programming Features
- Modularized code using functions and objects
- Typed data using case classes and Datasets
- Immutable values
- Clean, readable code with camelCase naming
- ScalaDocs on public functions and classes

## Configuration (`application.conf`)
The project uses a configuration file at `src/main/resources/application.conf` to manage input/output paths and Spark settings. You can adjust these parameters to fit your environment or requirements.

### Parameters
- **quantexa.input.flights**: Path to the flight data CSV file (default: `data/flightData.csv`).
- **quantexa.input.passengers**: Path to the passenger data CSV file (default: `data/passengers.csv`).
- **quantexa.output.mostFrequentFlyers**: Path for the output CSV file for most frequent flyers (default: `results/MostFrequentFlyers.csv`).
- **quantexa.output.singleCsv**: If `true`, outputs all results to a single CSV file (default: `true`).
- **quantexa.spark.master**: Spark master URL (default: `local[*]` for local execution).
- **quantexa.spark.appName**: Spark application name.
- **quantexa.spark.executor.memory**: Memory allocated per Spark executor (default: `16g`).
- **quantexa.spark.executor.cores**: Number of cores per Spark executor (default: `3`).
- **quantexa.spark.default.parallelism**: Default parallelism for Spark jobs (default: `32`).
- **quantexa.spark.sql.shuffle.partitions**: Number of partitions for shuffle operations (default: `200`).
- **quantexa.spark.serializer**: Serializer used by Spark (default: `KryoSerializer` for performance).
- **quantexa.spark.sql.autoBroadcastJoinThreshold**: Threshold for broadcast joins (default: `200MB`).

### How to Change Configuration
Edit `src/main/resources/application.conf` to change file paths or Spark settings. For example, to use a different input file, update:
```
quantexa.input.flights = "data/myFlights.csv"
```

### Notes
- For large datasets, you may want to increase memory, cores, or parallelism.
- The configuration is loaded automatically when you run the project.
- All output files are saved to the paths specified in this file.

## Further Documentation
For more details about the project structure and scalability enhancements, refer to:
- **ARCHITECTURE.md**: Overview of the system architecture, design decisions, and module interactions.
- **BIG_DATA_ENHANCEMENTS.md**: Description of optimizations and enhancements for handling large datasets and improving Spark performance.

## Contact
For any questions, please contact the author or refer to the assignment instructions.

## Repository
The source code for this project is also available on GitHub:
https://github.com/ismailhijazi/QuantexaAssignment
