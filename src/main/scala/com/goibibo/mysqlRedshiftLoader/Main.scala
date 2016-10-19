package com.goibibo.mysqlRedshiftLoader

import java.io.{File, InputStream}
import java.util.Properties

import com.goibibo.mysqlRedshiftLoader
import com.goibibo.mysqlRedshiftLoader.alerting.MailUtil
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.json4s._
import org.json4s.native.JsonMethods._
import org.slf4j.{Logger, LoggerFactory}
import scopt.OptionParser

/**
  * Entry point to RDS to Redshift data pipeline
  */
object Main {
    private val logger: Logger = LoggerFactory.getLogger(Main.getClass)
    private val parser: OptionParser[AppParams] =
        new OptionParser[AppParams]("Main") {
            head("RDS to Redshift DataPipeline")
            opt[String]("tableDetails")
                    .abbr("td")
                    .text("Table details json file path including")
                    .required()
                    .valueName("<path to Json>")
                    .action((x, c) => c.copy(tableDetailsPath = x))

            opt[String]("mailDetails")
                    .abbr("mail")
                    .text("Mail details property file path")
                    .optional()
                    .valueName("<path to properties file>")
                    .action((x, c) => c.copy(mailDetailsPath = x))

            //            opt[Int]("partitions")
            //                    .abbr("p")
            //                    .text("Partitions to be done(Optional)")
            //                    .valueName("<partitions>")
            //                    .action((x, c) => c.copy(partitions = Option(x)))
            //
            //            opt[Boolean]("targetRecordOverwrite")
            //                    .abbr("ro")
            //                    .text("To overwrite updated records in redshift")
            //                    .valueName("<true/false>")
            //                    .action((x, c) => c.copy(targetRecordOverwrite = x))
            //
            //            opt[Boolean]("targetRecordOverwrite")
            //                    .abbr("ro")
            //                    .text("To overwrite updated records in redshift")
            //                    .valueName("<true/false>")
            //                    .action((x, c) => c.copy(targetRecordOverwrite = x))
            //
            //            opt[String]("targetRecordOverwriteKey")
            //                    .abbr("rok")
            //                    .text("To overwrite updated records on particular keys in redshift")
            //                    .valueName("<key>")
            //                    .action((x, c) => c.copy(targetRecordOverwriteKey = Option(x)))

            help("help")
                    .text("Usage Of arguments")
        }

    private def getDBsConf(mysqlJson: JValue, redshiftJson: JValue, s3Json: JValue, table: JValue):
    (mysqlRedshiftLoader.DBConfiguration, mysqlRedshiftLoader.DBConfiguration, mysqlRedshiftLoader.S3Config) = {
        implicit val formats = DefaultFormats

        val mysqlConf: DBConfiguration = DBConfiguration("mysql", (mysqlJson \ "db").extract[String], null,
            (table \ "name").extract[String], (mysqlJson \ "hostname").extract[String],
            (mysqlJson \ "portno").extract[Int], (mysqlJson \ "username").extract[String],
            (mysqlJson \ "password").extract[String])

        val redshiftConf: DBConfiguration = DBConfiguration("redshift", "goibibo",
            (redshiftJson \ "schema").extract[String], (table \ "name").extract[String],
            (redshiftJson \ "hostname").extract[String], (redshiftJson \ "portno").extract[Int],
            (redshiftJson \ "username").extract[String], (redshiftJson \ "password").extract[String])

        val s3Conf: S3Config = S3Config((s3Json \ "location").extract[String],
            (s3Json \ "accessKey").extract[String], (s3Json \ "secretKey").extract[String])
        (mysqlConf, redshiftConf, s3Conf)
    }

    private def getAppConfigurations(jsonPath: String): Seq[AppConfiguration] = {
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
                val tables: List[JValue] = (detail \ "tables").extract[List[JValue]]
                for (table <- tables) {
                    val (mysqlConf: DBConfiguration, redshiftConf: DBConfiguration, s3Conf: S3Config) =
                        getDBsConf(mysqlJson, redshiftJson, s3Json, table)
                    logger.info("\n------------- Start :: table: {} -------------", (table \ "name").extract[String])
                    val incrementalColumn: JValue = table \ "incremental"
                    var internalConfig: InternalConfig = null

                    val isSplittableValue = table \ "isSplittable"
                    val isSplittable: Boolean = if (isSplittableValue != JNothing && isSplittableValue != JNull) {
                        isSplittableValue.extract[Boolean]
                    } else {
                        true
                    }
                    logger.info("Whether table is splittable: {}", isSplittable)

                    val partitionsValue = table \ "partitions"
                    var partitions: Int = 1
                    if (isSplittable) {
                        partitions = if (partitionsValue == JNothing || partitionsValue == JNull)
                            Util.getPartitions(mysqlConf)
                        else
                            partitionsValue.extract[Int]
                    }
                    logger.info("Number of partitions: {}", partitions)

                    if (incrementalColumn == JNothing || incrementalColumn == JNull) {
                        logger.info("Table is not incremental")
                        internalConfig = InternalConfig(shallSplit = Some(isSplittable), mapPartitions = partitions,
                            reducePartitions = partitions)
                    } else {
                        val whereCondition: String = incrementalColumn.extract[String]
                        logger.info("Table is incremental with condition: {}", whereCondition)
                        val mergeKeyValue: JValue = table \ "mergeKey"
                        val mergeKey: Option[String] = if (mergeKeyValue == JNothing || mergeKeyValue == JNull)
                            None
                        else
                            Some(mergeKeyValue.extract[String])
                        logger.info("Merge Key: {}", mergeKey.orNull)

                        val addColumnValue: JValue = table \ "addColumn"
                        val addColumn: Option[String] = if (addColumnValue == JNothing || addColumnValue == JNull)
                            None
                        else
                            Some(addColumnValue.extract[String])
                        logger.info("Add Column: {}", addColumn.orNull)

                        val incrementalSettings: IncrementalSettings = IncrementalSettings(whereCondition,
                            shallMerge = true, mergeKey = mergeKey, shallVacuumAfterLoad = true,
                            customSelectFromStaging = addColumn)

                        val settings: Some[IncrementalSettings] = Some(incrementalSettings)
                        internalConfig = InternalConfig(shallSplit = Some(isSplittable), incrementalSettings = settings,
                            mapPartitions = partitions, reducePartitions = partitions)
                    }
                    configurations = configurations :+ AppConfiguration(mysqlConf, redshiftConf, s3Conf, internalConfig)
                    logger.info("\n------------- End :: table: {} -------------", (table \ "name").extract[String])
                }
            }
        } finally {
            jsonInputStream.close()
        }
        configurations
    }

    private def run(sqlContext: SQLContext, configurations: Seq[AppConfiguration]): Unit = {
        implicit val crashOnInvalidValue: Boolean = true

        for (configuration <- configurations) {
            logger.info("Configuration: \n{}", configuration.toString)
            try {
                val mySqlTableName = s"${configuration.mysqlConf.db}.${configuration.mysqlConf.tableName}"
                val redshiftTableName = s"${configuration.redshiftConf.schema}.${configuration.mysqlConf.tableName}"
                sqlContext.sparkContext.setJobDescription(s"${mySqlTableName} -> ${redshiftTableName}")
                val loadedTable: (DataFrame, TableDetails) = mysqlSchemaExtractor.loadToSpark(configuration.mysqlConf,
                    sqlContext, configuration.internalConfig)
                if (loadedTable._1 == null) {
                    throw new Exception("Data frame is null.")
                }
                mysqlSchemaExtractor.storeToRedshift(loadedTable._1, loadedTable._2, configuration.redshiftConf,
                    configuration.s3Conf, sqlContext, configuration.internalConfig)
                logger.info("Successful transfer for configuration\n{}", configuration.toString)
                configuration.status = Some(Status(isSuccessful = true, None))
            } catch {
                case e: Exception =>
                    logger.info("Transfer Failed for configuration: \n{}", configuration)
                    logger.error("Stack Trace: ", e.fillInStackTrace())
                    configuration.status = Some(Status(isSuccessful = false, Some(e.getMessage + "\n" + e.getStackTraceString)))
            }
        }
    }

    def main(args: Array[String]): Unit = {
        logger.info("Reading Arguments")
        val appParams: AppParams = parser.parse(args, AppParams(null, null)).orNull
        if (appParams.tableDetailsPath == null) {
            logger.error("Table details json file is not provided!!!")
            throw new NullPointerException("Table details json file is not provided!!!")
        }

        val (_, sqlContext) = Util.getSparkContext

        logger.info("Getting all configurations")
        val configurations: Seq[AppConfiguration] = getAppConfigurations(appParams.tableDetailsPath)

        logger.info("Total number of tables to transfer are : {}", configurations.length)

        run(sqlContext, configurations)

        if (appParams.mailDetailsPath == null) {
            logger.info("Mail details properties file is not provided!!!")
            logger.info("Disabling alerting")
        } else {
            logger.info("Alerting developers....")
            val prop: Properties = new Properties()
            prop.load(new File(appParams.mailDetailsPath).toURI.toURL.openStream())
            val mailParams: MailParams = MailParams(prop.getProperty("alert.host"), null,
                prop.getProperty("alert.to"), prop.getProperty("alert.cc"))
            new MailUtil(mailParams).send(configurations.toList)
        }
    }
}