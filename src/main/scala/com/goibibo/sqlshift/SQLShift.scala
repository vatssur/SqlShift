package com.goibibo.sqlshift

import java.io.File
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.goibibo.sqlshift.alerting.{MailAPI, MailUtil}
import com.goibibo.sqlshift.commons.MetricsWrapper._
import com.goibibo.sqlshift.commons.{MySQLToRedshiftMigrator, Util}
import com.goibibo.sqlshift.models.Configurations.AppConfiguration
import com.goibibo.sqlshift.models.InternalConfs.{MigrationTime, TableDetails}
import com.goibibo.sqlshift.models.Params.AppParams
import com.goibibo.sqlshift.models.Status
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.{Logger, LoggerFactory}
import scopt.OptionParser

/**
  * Entry point to RDS to Redshift data pipeline
  */
object SQLShift {
    private val logger: Logger = LoggerFactory.getLogger(SQLShift.getClass)
    private val parser: OptionParser[AppParams] =
        new OptionParser[AppParams]("SQLShift") {
            head("MySQL to Redshift DataPipeline")
            opt[String]("table-details")
                    .abbr("td")
                    .text("Table details json file path including")
                    .required()
                    .valueName("<path to Json>")
                    .action((x, c) => c.copy(tableDetailsPath = x))

            opt[String]("mail-details")
                    .abbr("mail")
                    .text("Mail details property file path(For enabling mail)")
                    .optional()
                    .valueName("<path to properties file>")
                    .action((x, c) => c.copy(mailDetailsPath = x))

            opt[Unit]("alert-on-failure")
                    .abbr("aof")
                    .text("Alert only when fails")
                    .optional()
                    .action((_, c) => c.copy(alertOnFailure = true))

            opt[Int]("retry-count")
                    .abbr("rc")
                    .text("How many times to retry on failed transfers")
                    .optional()
                    .valueName("<count>")
                    .action((x, c) => c.copy(retryCount = x))

            opt[Unit]("log-metrics-reporter")
                    .abbr("lmr")
                    .text("Enable metrics reporting in logs")
                    .optional()
                    .action((x, c) => c.copy(logMetricsReporting = true))

            opt[Unit]("jmx-metrics-reporter")
                    .abbr("jmr")
                    .text("Enable metrics reporting through JMX")
                    .optional()
                    .action((x, c) => c.copy(jmxMetricsReporting = true))

            opt[Long]("metrics-window-size")
                    .abbr("mws")
                    .text("Metrics window size in seconds. Default: 5 seconds")
                    .optional()
                    .action((x, c) => c.copy(metricsWindowSize = x))

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
                val mySqlTableName = s"${configuration.mysqlConf.db}.${configuration.mysqlConf.tableName}"
                val redshiftTableName = s"${configuration.redshiftConf.schema}.${configuration.mysqlConf.tableName}"
                sqlContext.sparkContext.setJobDescription(s"$mySqlTableName => $redshiftTableName")
                val metricName = mySqlTableName + s".${configuration.redshiftConf.schema}"
                try {
                    val context = getTimerMetrics(s"$metricName.loadMetrics")
                    // Loading table
                    val loadedTable: (DataFrame, TableDetails) = MySQLToRedshiftMigrator.loadToSpark(configuration.mysqlConf,
                        sqlContext, configuration.internalConfig)
                    // For stopping lazy evaluation
                    loadedTable._1.count()
                    val loadTime: Long = TimeUnit.NANOSECONDS.toMillis(context.stop())

                    val storeTime: Long =
                        if (loadedTable._1 != null) {
                            // Storing table to redshift
                            val context = getTimerMetrics(s"$metricName.storeMetrics")
                            MySQLToRedshiftMigrator.storeToRedshift(loadedTable._1, loadedTable._2,
                                configuration.redshiftConf, configuration.s3Conf, sqlContext,
                                configuration.internalConfig)
                            TimeUnit.NANOSECONDS.toMillis(context.stop())
                        } else 0

                    logger.info("Successful transfer for configuration\n{}", configuration.toString)
                    configuration.status = Some(Status(isSuccessful = true, null))
                    configuration.migrationTime = Some(MigrationTime(loadTime = loadTime, storeTime = storeTime))
                    registerGauge(metricName = s"$metricName.migrationSuccess", value = 1)
                } catch {
                    case e: Exception =>
                        logger.info("Transfer Failed for configuration: \n{}", configuration)
                        logger.error("Stack Trace: ", e.fillInStackTrace())
                        configuration.status = Some(Status(isSuccessful = false, e))
                        configuration.migrationTime = Some(MigrationTime(loadTime = 0, storeTime = 0))
                        incCounter(s"$metricName.migrationFailedRetries")
                }
            }
        }
    }

    private def rerun(sqlContext: SQLContext, configurations: Seq[AppConfiguration], retryCount: Int): Unit = {
        var failedConfigurations: Seq[AppConfiguration] = null
        (1 to retryCount).foreach { retryCnt =>
            Util.exponentialPause(retryCnt)
            logger.info("Retry count: {}", retryCnt)
            failedConfigurations = Seq()
            for (configuration <- configurations) {
                if (configuration.status.isDefined && !configuration.status.get.isSuccessful) {
                    val metricName = s"${configuration.mysqlConf.db}.${configuration.mysqlConf.tableName}." +
                            s"${configuration.redshiftConf.schema}"
                    failedConfigurations = failedConfigurations :+ configuration
                }
            }
            if (failedConfigurations.isEmpty) {
                logger.info("All failed tables are successfully transferred due to VACUUM")
                return
            } else {
                run(sqlContext, failedConfigurations, isRetry = true)
            }
            incCounter(s"$retryCnt.FailedTransferRetry", failedConfigurations.size.toLong)
        }
        logger.info("Some failed tables are still left")
    }

    private def startMetrics(appParams: AppParams): Unit = {
        // Start metrics reporter
        if (appParams.logMetricsReporting) {
            startSLF4JReporting(appParams.metricsWindowSize)
        }

        if (appParams.jmxMetricsReporting) {
            startJMXReporting()
        }
    }

    def main(args: Array[String]): Unit = {
        logger.info("Reading Arguments")
        val appParams: AppParams = parser.parse(args, AppParams(null, null)).orNull
        if (appParams.tableDetailsPath == null) {
            logger.error("Table details json file is not provided!!!")
            throw new NullPointerException("Table details json file is not provided!!!")
        }

        // Start recording metrics
        startMetrics(appParams)

        val (_, sqlContext) = Util.getSparkContext

        logger.info("Getting all configurations")
        val configurations: Seq[AppConfiguration] = Util.getAppConfigurations(appParams.tableDetailsPath)

        logger.info("Total number of tables to transfer are : {}", configurations.length)

        incCounter("NumberOFTables", configurations.size.toLong)
        run(sqlContext, configurations, isRetry = false)
        if (appParams.retryCount > 0 && Util.anyFailures(configurations)) {
            logger.info("Found failed transfers with retry count: {}", appParams.retryCount)
            rerun(sqlContext, configurations, retryCount = appParams.retryCount)
        }

        //Closing Spark Context
        Util.closeSparkContext(sqlContext.sparkContext)
        logger.info("Info Section: \n{}", Util.formattedInfoSection(configurations))

        if (appParams.mailDetailsPath == null) {
            logger.info("Mail details properties file is not provided!!!")
            logger.info("Disabling alerting")
        } else if (!appParams.alertOnFailure || Util.anyFailures(configurations)) {
            logger.info("Alerting developers....")
            val prop: Properties = new Properties()
            prop.load(new File(appParams.mailDetailsPath).toURI.toURL.openStream())
            // Sending mail
            try {
                new MailAPI(MailUtil.getMailParams(prop)).send(configurations.toList)
            } catch {
                case e: Exception => logger.warn("Failed to send mail with reason: {}", e.getStackTrace.mkString("\n"))
            }
        }

        // For collecting metrics for last table
        Thread.sleep(appParams.metricsWindowSize * 1000)
    }
}