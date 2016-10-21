package com.goibibo.mysqlRedshiftLoader

import java.io.File
import java.util.Properties

import com.goibibo.mysqlRedshiftLoader.alerting.MailUtil
import org.apache.spark.sql.{DataFrame, SQLContext}
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
                    .text("Mail details property file path(For enabling mail)")
                    .optional()
                    .valueName("<path to properties file>")
                    .action((x, c) => c.copy(mailDetailsPath = x))

            opt[Unit]("alertOnFailure")
                    .abbr("aof")
                    .text("Alert only when fails")
                    .optional()
                    .action((_, c) => c.copy(alertOnFailure = true))

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

    private def run(sqlContext: SQLContext, configurations: Seq[AppConfiguration]): Unit = {
        implicit val crashOnInvalidValue: Boolean = true

        for (configuration <- configurations) {
            logger.info("Configuration: \n{}", configuration.toString)
            try {
                val mySqlTableName = s"${configuration.mysqlConf.db}.${configuration.mysqlConf.tableName}"
                val redshiftTableName = s"${configuration.redshiftConf.schema}.${configuration.mysqlConf.tableName}"
                sqlContext.sparkContext.setJobDescription(s"$mySqlTableName => $redshiftTableName")
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
        val configurations: Seq[AppConfiguration] = Util.getAppConfigurations(appParams.tableDetailsPath)

        logger.info("Total number of tables to transfer are : {}", configurations.length)

        run(sqlContext, configurations)

        if (appParams.mailDetailsPath == null) {
            logger.info("Mail details properties file is not provided!!!")
            logger.info("Disabling alerting")
        } else if (!appParams.alertOnFailure || Util.anyFailures(configurations)) {
            logger.info("Alerting developers....")
            val prop: Properties = new Properties()
            prop.load(new File(appParams.mailDetailsPath).toURI.toURL.openStream())
            val mailParams: MailParams = MailParams(prop.getProperty("alert.host"), null,
                prop.getProperty("alert.to"), prop.getProperty("alert.cc"))
            new MailUtil(mailParams).send(configurations.toList)
        }
        logger.info("Info Section: \n{}", Util.formattedInfoSection(configurations))
    }
}