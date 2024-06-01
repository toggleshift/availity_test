package com.availity.spark.provider
import java.nio.file.Paths
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}
import org.apache.spark.sql.functions.{col, concat, count, lit, month}

object ProviderRoster  {
  private[provider] val DataFolder = "data"

  /**
   * Read the provider dataset from the file system.
   *
   * @param spark SparkSession
   * @return DataFrame - provider dataset containing provider_id|provider_specialty|first_name|middle_name|last_name
   */
  private[provider] def readProviders(spark: SparkSession): DataFrame = {
    val providersDf = spark.read.format("csv")
      .option("delimiter", "|")
      .option("header", "true")
      .load(Paths.get(DataFolder, "providers.csv").toString)

    providersDf.show()
    providersDf
  }

  /**
   * Read the visits dataset from the file system.
   *
   * @param spark SparkSession
   * @return DataFrame - visits dataset containing visit_id|provider_id|date
   */
  private[provider] def readVisits(spark: SparkSession): DataFrame = {
    val visitsDf = spark.read
      .format("csv")
      .option("header", "false")
      .load(Paths.get(DataFolder, "visits.csv").toString)
      .withColumnRenamed("_c0", "visit_id")
      .withColumnRenamed("_c1", "provider_id")
      .withColumnRenamed("_c2", "date")

    visitsDf.show()
    visitsDf
  }

  /**
   * Given the two datasets, calculate the total number of visits per provider.
   * The resulting set should contain the provider's ID, name, specialty, along with
   * the number of visits. Output the report in json, partitioned by the provider's specialty.
   *
   * @param providersDf DataFrame - provider dataset (see definition above)
   * @param visitsDf DataFrame - visits dataset (see definition above)
   * @return DataFrame - containing provider_id|full_name|provider_specialty|num_visits
   */
  private[provider] def totalVisitsPerProvider(providersDf: DataFrame, visitsDf: DataFrame): DataFrame = {
    val totalVisitsDf = providersDf
      .withColumn("full_name", concat(col("first_name"), lit(" "), col("middle_name"), lit(" "), col("last_name")))
      .select("provider_id", "full_name", "provider_specialty")
      .join(visitsDf.groupBy("provider_id").agg(count("provider_id").as("num_visits")), Seq("provider_id"), "inner")

    totalVisitsDf.show()
    totalVisitsDf
  }

  /**
   * Given the two datasets, calculate the total number of visits per provider per month.
   * The resulting set should contain the provider's ID, the month, and total number of visits.
   * Output the result set in json.
   *
   * @param visitsDf DataFrame - visits dataset (see definition above)
   * @return DataFrame - containing provider_id|month|num_visits
   */
  private[provider] def totalVisitsPerProviderPerMonth(visitsDf: DataFrame): DataFrame = {
    val totalVisitsPerMonthDf = visitsDf
      .withColumn("month", month(col("date")))
      .groupBy("provider_id", "month")
      .agg(count("provider_id").as("num_visits"))

    totalVisitsPerMonthDf.show()
    totalVisitsPerMonthDf
  }

  /**
   * Output the results to the file system.
   *
   * @param df DataFrame - the result set to output
   * @param folder String - the folder to output the results to
   * @param partitionBy Option[String] - the optional column to partition the results by
   */
  private[provider] def outputResults(df: DataFrame, folder: String, partitionBy: Option[String]): Unit = {
    val path = Paths.get(DataFolder, folder).toString
    val partitionedWriter: DataFrameWriter[Row] = partitionBy.foldLeft(df.write)((writer, p) => writer.partitionBy(p))
    partitionedWriter.json(path)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Test App")
      .config("spark.master", "local")
      .getOrCreate()

    val providersDf = readProviders(spark)
    val visitsDf = readVisits(spark)

    val totalVisitsDf = totalVisitsPerProvider(providersDf, visitsDf)
    outputResults(totalVisitsDf, "output1", Some("provider_specialty"))

    val totalVisitsPerMonthDf = totalVisitsPerProviderPerMonth(visitsDf)
    outputResults(totalVisitsPerMonthDf, "output2", None)
  }
}
