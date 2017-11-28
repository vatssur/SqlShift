name := "sqlshift"
organization := "com.goibibo"
version := "0.1"
scalaVersion := "2.10.6"

resolvers ++= Seq(
    "Typesafe" at "http://repo.typesafe.com/typesafe/releases/",
    "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    "Cloudera" at "https://repository.cloudera.com/content/repositories/releases"
)


libraryDependencies ++= Seq(
    "mysql" % "mysql-connector-java" % "5.1.39",
    "org.apache.spark" %% "spark-core" % "1.6.0" % "provided",
    "org.apache.spark" %% "spark-sql" % "1.6.0" % "provided",
    ("com.databricks" %% "spark-redshift" % "1.1.0").
        exclude("org.apache.avro","avro").
        exclude("org.slf4j","slf4j-api"),
    ("com.amazonaws"           %   "aws-java-sdk-core"    % "1.10.22").
        exclude("com.fasterxml.jackson.core","jackson-core").
        exclude("com.fasterxml.jackson.core","jackson-databind").
        exclude("com.fasterxml.jackson.core","jackson-annotations").
        exclude("commons-codec", "commons-codec").
        exclude("commons-logging", "commons-logging").
        exclude("joda-time","joda-time").
        exclude("org.apache.httpcomponents","httpclient"),
    ("com.amazonaws" % "aws-java-sdk-s3" % "1.10.22").
        exclude("com.amazonaws","aws-java-sdk-core"),
    ("org.apache.hadoop"       % "hadoop-aws"            % "2.6.0-cdh5.8.2").
        exclude("com.amazonaws","aws-java-sdk-s3").
        exclude("com.fasterxml.jackson.core","jackson-databind").
        exclude("com.fasterxml.jackson.core","jackson-annotations")
        exclude("org.apache.hadoop","hadoop-common"),
    "com.github.scopt" %% "scopt" % "3.5.0",
    "javax.mail" % "mail" % "1.4.7",
    ("io.dropwizard" % "dropwizard-metrics" % "1.0.5").
        exclude("org.slf4j","slf4j-api")
)

unmanagedJars in Compile += file("lib/RedshiftJDBC4-1.1.17.1017.jar")

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

