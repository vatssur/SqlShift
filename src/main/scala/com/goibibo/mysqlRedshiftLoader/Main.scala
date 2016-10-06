package com.goibibo.mysqlRedshiftLoader

import java.io.{File, InputStream}
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s._
import org.json4s.native.JsonMethods._
import org.slf4j.{Logger, LoggerFactory}
import scopt.OptionParser

object Main {
    private val logger: Logger = LoggerFactory.getLogger(Main.getClass)
    private val parser: OptionParser[AppParams] =
        new OptionParser[AppParams]("Main") {
            head("RDS to Redshift DataPipeline")
            opt[String]("tableDetails")
                    .abbr("t")
                    .text("Table details json file path including ")
                    .action((x, c) => c.copy(tableDetailsPath = x))

            help("help")
                    .text("Usage Of arguments")
        }

    def getAppConfigurations(jsonPath: String, prop: Properties): Seq[AppConfiguration] = {
        var configurations: Seq[AppConfiguration] = Seq[AppConfiguration]()
        implicit val formats = DefaultFormats

        val jsonInputStream: InputStream = new File(jsonPath).toURI.toURL.openStream()
        try {
            val json: JValue = parse(jsonInputStream)
            val details: List[JValue] = json.extract[List[JValue]]
            for (detail <- details) {
                val mysqlJson: JValue = (detail \ "mysql").extract[JValue]
                val redshiftJson: JValue = (detail \ "redshift").extract[JValue]
                val s3Json: JValue = (detail \ "s3").extract[JValue]
                val tables: List[String] = (detail \ "tables").extract[List[String]]
                for (table <- tables) {
                    val mysqlConf: DBConfiguration = DBConfiguration("mysql", (mysqlJson \ "db").extract[String], null,
                        table, (mysqlJson \ "hostname").extract[String], (mysqlJson \ "portno").extract[Int],
                        (mysqlJson \ "username").extract[String], (mysqlJson \ "password").extract[String])

                    val redshiftConf: DBConfiguration = DBConfiguration("redshift", "goibibo",
                        (redshiftJson \ "schema").extract[String], table, (redshiftJson \ "hostname").extract[String],
                        (redshiftJson \ "portno").extract[Int], (redshiftJson \ "username").extract[String],
                        (redshiftJson \ "password").extract[String])

                    val s3Conf: S3Config = S3Config((s3Json \ "location").extract[String],
                        (s3Json \ "accessKey").extract[String], (s3Json \ "secretKey").extract[String])
                    configurations = configurations :+ AppConfiguration(mysqlConf, redshiftConf, s3Conf)
                }
            }
        } finally {
            jsonInputStream.close()
        }
        configurations
    }

    def run(configurations: Seq[AppConfiguration]): Unit = {
        logger.info("Starting spark context...")
        val sparkConf: SparkConf = new SparkConf().setAppName("RDS to Redshift DataPipeline")
        val sc: SparkContext = new SparkContext(sparkConf)
        val sqlContext: SQLContext = new SQLContext(sc)
        System.setProperty("com.amazonaws.services.s3.enableV4", "true")
        sc.hadoopConfiguration.set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")

        implicit val crashOnInvalidValue: Boolean = true

        for (configuration <- configurations) {
            logger.info("Configuration: \n{}", configuration.toString)
            try {
                val loadedTable: (DataFrame, TableDetails) = mysqlSchemaExtractor.loadToSpark(configuration.mysqlConf,
                    sqlContext)
                mysqlSchemaExtractor.storeToRedshift(loadedTable._1, loadedTable._2, configuration.redshiftConf,
                    configuration.s3Conf, sqlContext)(12)
                logger.info("Successful transfer for configuration\n{}", configuration.toString)
            } catch {
                case e: Exception =>
                    logger.info("Transfer Failed for configuration: \n{}", configuration)
                    logger.error("Stack Trace: ", e.fillInStackTrace())
            }
        }
    }

    def main(args: Array[String]): Unit = {
        val properties: Properties = new Properties(System.getProperties)
        logger.info("Reading Arguments")
        val appParams: AppParams = parser.parse(args, AppParams(null, null, null, null)).orNull
        if (appParams.tableDetailsPath == null) {
            logger.error("Table details is not provided is null!!!")
            throw new NullPointerException("Table details is not provided is null!!!")
        }
        if (appParams.mysqlConfPath != null) {
            logger.info("MySQL conf path is {}", appParams.mysqlConfPath)
            properties.load(new File(appParams.mysqlConfPath).toURI.toURL.openStream())
        }
        if (appParams.s3ConfPath != null) {
            logger.info("S3 conf path is {}", appParams.s3ConfPath)
            properties.load(new File(appParams.s3ConfPath).toURI.toURL.openStream())
        }
        if (appParams.redshiftConfPath != null) {
            logger.info("Redshift conf path is {}", appParams.redshiftConfPath)
            properties.load(new File(appParams.redshiftConfPath).toURI.toURL.openStream())
        }

        logger.info("Getting all configurations")
        val configurations: Seq[AppConfiguration] = getAppConfigurations(appParams.tableDetailsPath, properties)

        logger.info("Total number of transfers : {}", configurations.length)
        run(configurations)
    }
}