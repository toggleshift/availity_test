package com.availity.spark.provider

import com.availity.spark.provider.ProviderRoster.DataFolder
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers.{be, contain}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.nio.file.{Files, Paths}
import java.io.File
import scala.reflect.io.Directory

trait SparkSessionTester {
  protected val TestResultsFolder = "unit_test_results"
  protected val spark: SparkSession = SparkSession.builder
    .appName("Unit Test")
    .config("spark.master", "local[1]")
    .getOrCreate()
}

class ProviderRosterSpec extends AnyFunSpec with SparkSessionTester with DataFrameComparer with BeforeAndAfterEach {

  import spark.implicits._

  private val testResultsPath = Paths.get(DataFolder, TestResultsFolder)

  private val providersDf = Seq(
    ("11111", "Urology", "First1", "A", "Last1"),
    ("22222", "Cardiology", "First2", "B", "Last2"),
    ("33333", "Psychiatry", "First3", "C", "Last3"),
    ("44444", "Dermatology", "First4", "D", "Last4"),
  ).toDF("provider_id", "provider_specialty", "first_name", "middle_name", "last_name")

  private val visitsDf = Seq(
    ("10000001", "11111", "2021-01-01"),
    ("10000002", "11111", "2021-02-15"),
    ("10000003", "11111", "2021-03-31"),
    ("10000004", "22222", "2021-01-31"),
    ("10000005", "22222", "2021-02-01"),
    ("10000006", "22222", "2021-03-15"),
    ("10000007", "33333", "2021-01-01"),
    ("10000008", "33333", "2021-02-28"),
    ("10000009", "33333", "2021-03-15"),
    ("10000010", "55555", "2021-01-01"),
    ("10000011", "55555", "2021-02-15"),
    ("10000012", "55555", "2021-03-01"),
    ("10000013", "55555", "2021-03-15"),
    ("10000014", "55555", "2021-03-31"),
  ).toDF("visit_id", "provider_id", "date")

  override def beforeEach(): Unit = {
    Directory(new File(testResultsPath.toString)).deleteRecursively()
    Files.createDirectories(testResultsPath)
  }

  override def afterEach(): Unit = {
    Directory(new File(testResultsPath.toString)).deleteRecursively()
  }

  describe("readProviders") {
    val result = ProviderRoster.readProviders(spark)
    it("should contain the expected columns") {
      result.columns should contain theSameElementsAs Seq(
        "provider_id", "provider_specialty", "first_name", "middle_name", "last_name"
      )
    }
    it("should contain a nonzero number of rows") {
      result.count() should be > 0L
    }
  }

  describe("readVisits") {
    val result = ProviderRoster.readVisits(spark)
    it("should contain the expected columns") {
      result.columns should contain theSameElementsAs Seq(
        "visit_id", "provider_id", "date"
      )
    }
    it("should contain a nonzero number of rows") {
      result.count() should be > 0L
    }
  }

  describe("totalVisitsPerProvider") {

    val expectedDf = Seq(
      ("11111", "First1 A Last1", "Urology", 3),
      ("22222", "First2 B Last2", "Cardiology", 3),
      ("33333", "First3 C Last3", "Psychiatry", 3),
    ).toDF("provider_id", "full_name", "provider_specialty", "num_visits")

    val result = ProviderRoster.totalVisitsPerProvider(providersDf, visitsDf)
    it("should contain the expected columns") {
      result.columns should contain theSameElementsAs Seq(
        "provider_id", "full_name", "provider_specialty", "num_visits"
      )
    }
    
    it("should contain a nonzero number of rows") {
      result.count() should be > 0L
    }

    it("should contain correct data") {
      result.collect() should contain theSameElementsAs (expectedDf.collect())
    }
  }

  describe("totalVisitsPerProviderPerMonth") {

    val expectedDf = Seq(
      ("11111", 1, 1L),
      ("11111", 2, 1L),
      ("11111", 3, 1L),
      ("22222", 1, 1L),
      ("22222", 2, 1L),
      ("22222", 3, 1L),
      ("33333", 1, 1L),
      ("33333", 2, 1L),
      ("33333", 3, 1L),
      ("55555", 1, 1L),
      ("55555", 2, 1L),
      ("55555", 3, 3L),
    ).toDF("provider_id", "month", "num_visits")

    val result = ProviderRoster.totalVisitsPerProviderPerMonth(visitsDf)
    it("should contain the expected columns") {
      result.columns should contain theSameElementsAs Seq(
        "provider_id", "month", "num_visits"
      )
    }

    it("should contain a nonzero number of rows") {
      result.count() should be > 0L
    }

    it("should contain correct data") {
      result.collect() should contain theSameElementsAs (expectedDf.collect())
    }
  }

  describe("outputResults") {

    it("when not partitioned should contain JSON files directly in the output folder") {
      val nonPartitionedOutputFolder = Paths.get(testResultsPath.toString, "output_results_np")
      ProviderRoster.outputResults(visitsDf, Paths.get(TestResultsFolder, "output_results_np").toString, None)
      Files.newDirectoryStream(nonPartitionedOutputFolder, "provider_id=*").iterator().hasNext should be(false)
      Files.newDirectoryStream(nonPartitionedOutputFolder, "*.json").iterator().hasNext should be(true)
    }

    it("when partitioned should contain partition folders directly in the output folder") {
      val partitionedOutputFolder = Paths.get(testResultsPath.toString, "output_results_p")
      ProviderRoster.outputResults(visitsDf, Paths.get(TestResultsFolder, "output_results_p").toString, Some("provider_id"))
      Files.newDirectoryStream(partitionedOutputFolder, "provider_id=*").iterator().hasNext should be(true)
      Files.newDirectoryStream(partitionedOutputFolder, "*.json").iterator().hasNext should be(false)
    }
  }
}
