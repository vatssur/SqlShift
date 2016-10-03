name := "mysql-redshift-loader"
organization := "com.goibibo"
version := "0.1"
scalaVersion := "2.10.6"
resolvers ++= Seq(
  "Typesafe" at "http://repo.typesafe.com/typesafe/releases/",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
  )


libraryDependencies ++= Seq(
  "mysql" % "mysql-connector-java" % "5.1.39" % "provided",
  "org.apache.spark"        %%  "spark-core"      % "1.6.0" % "provided",
  "org.apache.spark"        %%  "spark-sql"       % "1.6.0" % "provided",
  "com.databricks"          %%  "spark-redshift"  % "1.1.0" % "provided",
  "com.amazonaws"           %   "aws-java-sdk"    % "1.7.4" % "provided",
  "org.apache.hadoop"       %   "hadoop-aws"      % "2.7.2" % "provided"
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

