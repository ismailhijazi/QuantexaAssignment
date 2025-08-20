# Big Data Enhancements Summary

This document outlines the key enhancements made to optimize the QuantexaAssignment project for big data scenarios using Spark/Scala.

## 1. Configuration Improvements
- **Increased Spark Executor Memory:**
  - `spark.executor.memory = "16g"` 
- **Higher Parallelism:**
  - `spark.default.parallelism = 32` 
  - `spark.sql.shuffle.partitions = 200` 
- **Kryo Serialization:**
  - `spark.serializer = "org.apache.spark.serializer.KryoSerializer"`
- **Broadcast Join Optimization:**
  - `spark.sql.autoBroadcastJoinThreshold = "200MB"`
- **Output Handling:**
  - `singleCsv = false` to avoid bottlenecks and enable parallel writes

## 2. Code-Level Enhancements
- **CsvUtils:**
  - Avoids `coalesce(1)` for large outputs, enabling parallel file writes.
  - Warns if single file output is requested for big data.
- **ParquetUtils:**
  - Added for efficient columnar storage and fast reads/writes.
  - Supports conversion from CSV to Parquet and compressed output.
  - **Usage Example:**
    ```scala
    import QuantexaAssignment.util.ParquetUtils
    ParquetUtils.convertCsvToParquet(spark, "data/flightData.csv", "data/flightData.parquet", schema)
    ```
- **PassengersFlownTogether:**
  - Refactored to use DataFrame API for much faster performance and scalability.
  - Legacy RDD method enhanced for efficiency if needed.
- **DataValidationUtils:**
  - Validates input data for schema correctness and non-empty datasets before analytics are run.
  - Provides extensible data quality rules (e.g., duplicate detection).
  - Integrated in the main workflow to ensure robust data quality and prevent downstream errors.
  - Located at `src/main/scala/util/DataValidationUtils.scala`.

## 3. Architectural & Workflow Enhancements
- **Functional Programming Best Practices:**
  - Immutable data structures, pure functions, and modular analytics objects.
- **Modular Utilities:**
  - Clear separation of concerns: CsvUtils for CSV, ParquetUtils for Parquet.
- **Flexible Configuration:**
  - Uses Typesafe Config for easy tuning and environment adaptation.
  - **Switching Output Mode:** Set `quantexa.output.singleCsv = false` in `application.conf` to enable parallel output files for big data.
- **Documentation:**
  - README.md and ARCHITECTURE.md updated to reflect all enhancements and usage patterns.
  - For more details, see the referenced documentation files.

## 4. Best Practices & Recommendations
- **Use Parquet for Large Datasets:**
  - Parquet files are faster to read (5-10x) and take up less space (50-80% smaller) compared to CSVs.
- **Avoid Single File Output for Big Data:**
  - Instead of writing everything to one file, let Spark create multiple smaller files. This makes the process faster and more scalable.
- **Leverage Broadcast Joins:**
  - If one side of your join is small (less than 200MB), use broadcast joins to speed things up.
- **Tune Partitioning:**
  - Adjust the number of partitions and parallelism settings to match your cluster size and data volume.
- **Monitor Resource Usage:**
  - Keep an eye on Spark's UI to spot bottlenecks. Increase executor memory if needed.

## 5. Future Enhancements
- **Add Real-Time Analytics:**
  - Use Spark Streaming to handle data as it comes in, instead of processing it in batches.
- **Set Up Automated Testing and Deployment:**
  - Add CI/CD pipelines to make testing and deployment smoother and faster.
- **Compress Output Files:**
  - Save storage space and speed up file operations by compressing output files with formats like Gzip or Snappy.
