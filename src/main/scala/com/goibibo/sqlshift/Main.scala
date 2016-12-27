package com.goibibo.sqlshift

import java.io.File
import java.util.Properties

import com.goibibo.sqlshift.alerting.MailUtil
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

            opt[Int]("retryCount")
                    .abbr("rc")
                    .text("How many times to retry on failed transfers")
                    .optional()
                    .valueName("<count>")
                    .action((x, c) => c.copy(retryCount = x))

            help("help")
                    .text("Usage Of arguments")
        }

    private def run(sqlContext: SQLContext, configurations: Seq[AppConfiguration], isRetry: Boolean): Unit = {
        implicit val crashOnInvalidValue: Boolean = true

        for (configuration <- configurations) {
            if (!isRetry && configuration.status.isDefined && !configuration.status.get.isSuccessful) {
                val e: Exception = configuration.status.get.e
                logger.warn(s"Skipping configuration: ${configuration.toString} " +
                        s"with reason: ${e.getMessage}\n${e.getStackTraceString}")
            } else {
                logger.info("Configuration: \n{}", configuration.toString)
                try {
                    val mySqlTableName = s"${configuration.mysqlConf.db}.${configuration.mysqlConf.tableName}"
                    val redshiftTableName = s"${configuration.redshiftConf.schema}.${configuration.mysqlConf.tableName}"
                    sqlContext.sparkContext.setJobDescription(s"$mySqlTableName => $redshiftTableName")
                    val loadedTable: (DataFrame, TableDetails) = MySQLToRedshiftMigrator.loadToSpark(configuration.mysqlConf,
                        sqlContext, configuration.internalConfig)
                    if (loadedTable._1 != null) {
                        MySQLToRedshiftMigrator.storeToRedshift(loadedTable._1, loadedTable._2, configuration.redshiftConf,
                            configuration.s3Conf, sqlContext, configuration.internalConfig)
                    }
                    logger.info("Successful transfer for configuration\n{}", configuration.toString)
                    configuration.status = Some(Status(isSuccessful = true, null))
                } catch {
                    case e: Exception =>
                        logger.info("Transfer Failed for configuration: \n{}", configuration)
                        logger.error("Stack Trace: ", e.fillInStackTrace())
                        configuration.status = Some(Status(isSuccessful = false, e))
                }
            }
        }
    }

    private def rerun(sqlContext: SQLContext, configurations: Seq[AppConfiguration], retryCount: Int): Unit = {
        var retryCnt: Int = retryCount
        var failedConfigurations: Seq[AppConfiguration] = null
        while (retryCnt > 0) {
            logger.info("Retrying with retry count: {}", retryCnt)
            failedConfigurations = Seq()
            for (configuration <- configurations) {
                if (configuration.status.isDefined && !configuration.status.get.isSuccessful &&
                        configuration.status.get.e.getMessage.contains("VACUUM is running")) {
                    failedConfigurations = failedConfigurations :+ configuration
                }
            }
            if (failedConfigurations.isEmpty) {
                logger.info("All failed tables are successfully transferred due to VACUUM")
                return
            } else {
                run(sqlContext, failedConfigurations, isRetry = true)
            }
            retryCnt -= 1
        }
        logger.info("Some failed tables are still left")
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

        run(sqlContext, configurations, isRetry = false)
        if (appParams.retryCount > 0 && Util.anyFailures(configurations)) {
            logger.info("Found failed transfers with retry count: {}", appParams.retryCount)
            rerun(sqlContext, configurations, retryCount = appParams.retryCount)
        }
        sqlContext.sparkContext.stop
        if (appParams.mailDetailsPath == null) {
            logger.info("Mail details properties file is not provided!!!")
            logger.info("Disabling alerting")
        } else if (!appParams.alertOnFailure || Util.anyFailures(configurations)) {
            logger.info("Alerting developers....")
            val prop: Properties = new Properties()
            prop.load(new File(appParams.mailDetailsPath).toURI.toURL.openStream())
            val mailParams: MailParams = MailParams(prop.getProperty("alert.host"),
                null,
                prop.getProperty("alert.to", ""),
                prop.getProperty("alert.cc", ""),
                prop.getProperty("alert.subject", ""))
            new MailUtil(mailParams).send(configurations.toList)
        }
        logger.info("Info Section: \n{}", Util.formattedInfoSection(configurations))
    }
}