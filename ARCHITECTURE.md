# QuantexaAssignment Project Architecture

## 1. Introduction & Overview
QuantexaAssignment is a Spark/Scala project for flight and passenger analytics, written in a functional style and following best practices for modularity, clarity, and maintainability. The solution leverages Spark for distributed, scalable analytics and uses the Scala standard library where appropriate. All analytics modules are implemented in Scala, with careful consideration for scalability and performance, making the solution suitable for both small and very large datasets. Robust configuration is provided via Typesafe Config, ensuring flexibility and ease of extension.

## 2. Technologies & Dependencies
- **Language:** Scala 2.12.10
- **Framework:** Apache Spark 2.4.8
- **Build Tool:** SBT
- **Config:** Typesafe Config
- **Testing:** ScalaTest
- **Logging:** Log4j
- **Data Format:** CSV, Parquet (added for performance)

## 3. Directory Structure
```
SbtExampleProject/
├── build.sbt
├── README.md
├── data/
│   ├── flightData.csv
│   └── passengers.csv
├── results/
│   └── [output CSVs]
├── src/
│   ├── main/
│   │   ├── resources/
│   │   │   └── application.conf
│   │   └── scala/
│   │       ├── Main.scala
│   │       ├── analytics/
│   │       ├── model/
│   │       └── util/
│   └── test/
│       └── scala/
│           ├── analytics/
│           ├── util/
└── target/
```
- **main/scala:** Source code (analytics, models, utilities)
- **test/scala:** Unit tests
- **resources:** Configuration files
- **data:** Input CSVs
- **results:** Output CSVs

## 4. Configuration System
- Managed via `application.conf` (Typesafe Config)
- All config keys documented in README.md
- Example:
```hocon
quantexa.input.flights = "data/flightData.csv"
quantexa.input.passengers = "data/passengers.csv"
quantexa.output.mostFrequentFlyers = "results/MostFrequentFlyers.csv"
quantexa.output.singleCsv = true
quantexa.spark.master = "local[*]"
quantexa.spark.appName = "QuantexaAssignment"
quantexa.spark.executor.memory = "16g"
quantexa.spark.executor.cores = 3
quantexa.spark.default.parallelism = 32
quantexa.spark.sql.shuffle.partitions = 200
quantexa.spark.serializer = "org.apache.spark.serializer.KryoSerializer"
quantexa.spark.sql.autoBroadcastJoinThreshold = "200MB"
```

## 5. Analytics Orchestration
- The main workflow is managed by `Main.scala`, which loads configuration, validates input data, and runs the selected analytics module.
- Data validation is performed before analytics to ensure schema and non-empty datasets.
- Results are written to CSV (or Parquet for large data) as configured.

## 6. Testing
- Unit tests are located in `src/test/scala/analytics/` and `src/test/scala/util/`.
- Tests cover utility functions and analytics modules for correctness and robustness.

## 7. Error Handling & Logging
- Uses Try for error handling in data loading and analytics
- Logs errors and info via Log4j
- Handles missing files, schema mismatches, empty datasets

## 8. Extensibility & Maintenance
- Add new analytics by creating new objects in `analytics/`
- Add new config keys in `application.conf` and document in README.md
- Utilities are reusable and modular

## 9. Example Workflow
1. Place input CSVs in `data/`
2. (Optional) Convert CSVs to Parquet for best performance using ParquetUtils
3. Configure `application.conf` (see below for new keys)
4. Run analytics via SBT:
   ```sh
   sbt "run MostFrequentFlyers 100 --show"
   ```
5. Check results in `results/`
