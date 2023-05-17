import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.{ConstrainableDataTypes, ConstraintStatus}
import org.apache.spark.sql.SparkSession

import java.io.File
import scala.util.matching.Regex

object DatasetAnalyzer {
  def main(args: Array[String]): Unit = {

    //Download dataset from S3 before tests
    S3Downloader.main(Array.empty)
    val tsvFile = "amazon_reviews_us_Camera_v1_00.tsv"

    //Create spark session
    val spark = SparkSession.builder()
      .appName("test")
      .master("local[*]")
      .getOrCreate()

    // Read the .tsv file using Spark
    val rawDF = spark.read
      .option("header", "true")
      .option("delimiter", "\t")
      .option("inferSchema", "false")
      .csv(tsvFile)

    // Define data quality checks using Deequ
    val verificationResult: VerificationResult = VerificationSuite()
      .onData(rawDF)
      .addCheck(
        Check(CheckLevel.Error, "Data Validation Check")
          .isContainedIn("verified_purchase", Array("N", "Y")) // Check verified_purchase values
          .hasPattern("review_date", new Regex("\\d{4}-\\d{2}-\\d{2}")) // Perform regex matching
          .isUnique("review_id") // Ensure review_id values are unique
          .isComplete("review_id") // Ensure review_id values are non-null
          .hasDataType("total_votes", ConstrainableDataTypes.Numeric) // Check total_votes is an integer
      )
      .run()

    // Print the check results
    if (verificationResult.status == CheckStatus.Success) {
      println("The data passed the test, everything is fine!")
    } else {
      println("We found errors in the data:\n")

      val resultsForAllConstraints = verificationResult.checkResults
        .flatMap { case (_, checkResult) => checkResult.constraintResults }

      resultsForAllConstraints
        .filter {
          _.status != ConstraintStatus.Success
        }
        .foreach { result => println(s"${result.constraint}: ${result.message.get}") }
    }

    //Delete downloaded file:
    val downloadedFile = new File(tsvFile)
    if (downloadedFile.exists() && downloadedFile.isFile) {
      downloadedFile.delete()
      println("downloaded file deleted.")
    }
  }
}
