name := "mysql-redshift-loader"
organization := "com.goibibo"
version := "0.1"
scalaVersion := "2.10.6"

libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.39"
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.39"


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

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => { MergeStrategy.discard }
  case x => MergeStrategy.first
}

