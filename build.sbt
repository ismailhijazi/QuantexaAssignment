ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "QuantexaAssignment",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.4.8",
      "org.apache.spark" %% "spark-sql" % "2.4.8",
      "com.typesafe" % "config" % "1.4.2",
      "org.scalatest" %% "scalatest" % "3.2.17" % Test,

      // Add Parquet support for better performance
      "org.apache.parquet" % "parquet-hadoop" % "1.10.1"
    )
  )
