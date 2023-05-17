import sys.process._
import java.io.{File, FileOutputStream}
import java.util.zip.GZIPInputStream

object S3Downloader {
  def main(args: Array[String]): Unit = {
    val s3Bucket = "amazon-reviews-pds"
    val s3Key = "tsv/amazon_reviews_us_Camera_v1_00.tsv.gz"
    val localFilePath = "amazon_reviews_us_Camera_v1_00.tsv.gz"

    // Execute the AWS CLI command to download the file
    val command = s"aws s3 cp --no-sign-request s3://$s3Bucket/$s3Key $localFilePath"
    val exitCode = command.!

    if (exitCode == 0) {
      println("File downloaded successfully.")
      println(s"Local file path: $localFilePath")

      // Unpack the downloaded .gz file
      val unpackedFilePath = "amazon_reviews_us_Camera_v1_00.tsv"
      unpackGzFile(localFilePath, unpackedFilePath)

      println("File unpacked successfully.")
      println(s"Unpacked file path: $unpackedFilePath")

      // Delete the .gz file
      val gzFile = new File(localFilePath)
      if (gzFile.exists() && gzFile.isFile) {
        gzFile.delete()
        println("GZ file deleted.")
      }
    } else {
      println("File download failed.")
    }
  }

  def unpackGzFile(gzFilePath: String, unpackedFilePath: String): Unit = {
    val gzFile = new File(gzFilePath)
    val unpackedFile = new File(unpackedFilePath)

    val inputStream = new GZIPInputStream(new java.io.FileInputStream(gzFile))
    val outputStream = new FileOutputStream(unpackedFile)

    val buffer = new Array[Byte](1024)
    var length = inputStream.read(buffer)

    while (length > 0) {
      outputStream.write(buffer, 0, length)
      length = inputStream.read(buffer)
    }

    inputStream.close()
    outputStream.close()
  }
}
