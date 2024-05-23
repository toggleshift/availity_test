package com.availity.spark.provider
import java.nio.file.Paths
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions.{concat, count, lit, col, month, avg}

object ProviderRoster  {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Test App")
      .config("spark.master", "local")
      .getOrCreate()

    val providersDf = spark.read.format("csv")
      .option("delimiter", "|")
      .option("header", "true")
      .load(Paths.get("data", "providers.csv").toString)

    providersDf.show()

    val visitsDf = spark.read
      .format("csv")
      .option("header", "false")
      .load("data/visits.csv")
      .withColumnRenamed("_c0", "visit_id")
      .withColumnRenamed("_c1", "provider_id")
      .withColumnRenamed("_c2", "date")

    visitsDf.show()

    // Given the two data datasets, calculate the total number of visits per provider.
    // The resulting set should contain the provider's ID, name, specialty, along with
    // the number of visits. Output the report in json, partitioned by the provider's specialty.

    val output1Df = providersDf
      .withColumn("full_name", concat(col("first_name"), lit(" "), col("middle_name"), lit(" "), col("last_name")))
      .select("provider_id", "full_name", "provider_specialty")
      .join(visitsDf.groupBy("provider_id").agg(count("provider_id").as("num_visits")), Seq("provider_id"), "inner")

    output1Df.show()
    output1Df.write
      .partitionBy("provider_specialty")
      .json(Paths.get("data", "output1").toString)

    // Given the two datasets, calculate the total number of visits per provider per month.
    // The resulting set should contain the provider's ID, the month, and total number of visits.
    // Output the result set in json.

    val output2Df = visitsDf
      .withColumn("month", month(col("date")))
      .groupBy("provider_id", "month")
      .agg(count("provider_id").as("num_visits"))

    output2Df.show()
    output2Df.write.json(Paths.get("data", "output2").toString)
  }
}
