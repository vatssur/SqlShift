name := "mysql-redshift-loader"
organization := "com.goibibo"
version := "0.1"
scalaVersion := "2.10.6"

resolvers ++= Seq(
    "Typesafe" at "http://repo.typesafe.com/typesafe/releases/",
    "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
)

mergeStrategy in assembly := {
    case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
    case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
    case "log4j.properties" => MergeStrategy.discard
    case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
    case "reference.conf" => MergeStrategy.concat
    case _ => MergeStrategy.first
}

libraryDependencies ++= Seq(
    "mysql" % "mysql-connector-java" % "5.1.39",
    "org.apache.spark" %% "spark-core" % "1.6.0" % "provided",
    "org.apache.spark" %% "spark-sql" % "1.6.0" % "provided",
    "com.databricks" %% "spark-redshift" % "1.1.0",
    "com.amazonaws" % "aws-java-sdk" % "1.7.4",
    "org.apache.hadoop" % "hadoop-aws" % "2.7.2",
    "com.github.scopt" %% "scopt" % "3.5.0",
    "org.json4s" %% "json4s-native" % "3.4.1",
    "org.json4s" %% "json4s-jackson" % "3.4.1",
    "org.slf4j" % "slf4j-simple" % "1.7.21",
    "javax.mail" % "mail" % "1.4.7"
)

scalacOptions ++= Seq(
    "-encoding", "UTF-8",
    "-unchecked",
    "-deprecation",
    "-Xfuture",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Ywarn-value-discard"
)

