name := "deequPractice"

version := "0.1"

scalaVersion := "2.12.17"

libraryDependencies += "com.amazon.deequ" % "deequ" % "2.0.1-spark-3.2"
libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-aws" % "3.3.1",
  "software.amazon.awssdk" % "s3" % "2.15.78"
)


