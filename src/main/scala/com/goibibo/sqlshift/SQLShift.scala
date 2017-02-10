package com.goibibo.sqlshift

import java.io.{File, PrintWriter}
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.goibibo.sqlshift.alerting.{MailAPI, MailUtil}
import com.goibibo.sqlshift.commons.MetricsWrapper._
import com.goibibo.sqlshift.commons.{MySQLToRedshiftMigrator, Util}
import com.goibibo.sqlshift.models.Configurations.AppConfiguration
import com.goibibo.sqlshift.models.InternalConfs.TableDetails
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
                    .text("How many times to retry on failed transfers(Count should be less than equal to 9)")
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

    /**
      * Transfer tables provided in configurations and return list of configurations with status and transfer time.
      *
      * @param sqlContext     Spark sql context
      * @param configurations List of configurations which needs to be executed
      * @return List of configurations with status and transfer time.
      */
    def run(sqlContext: SQLContext, configurations: Seq[AppConfiguration]): Seq[AppConfiguration] = {
        implicit val crashOnInvalidValue: Boolean = true
        var finalConfigurations: Seq[AppConfiguration] = Seq[AppConfiguration]()

        for (configuration <- configurations) {
            logger.info("Configuration: \n{}", configuration.toString)
            val mySqlTableName = s"${configuration.mysqlConf.db}.${configuration.mysqlConf.tableName}"
            val redshiftTableName = s"${configuration.redshiftConf.schema}.${configuration.mysqlConf.tableName}"
            sqlContext.sparkContext.setJobDescription(s"$mySqlTableName => $redshiftTableName")
            val metricName = mySqlTableName + s".${configuration.redshiftConf.schema}"
            try {
                val context = getTimerMetrics(s"$metricName.migrationMetrics")
                // Loading table
                val loadedTable: (DataFrame, TableDetails) = MySQLToRedshiftMigrator.loadToSpark(configuration.mysqlConf,
                    sqlContext, configuration.internalConfig)
                    if (loadedTable._1 != null) {
                        // Storing table to redshift
                        MySQLToRedshiftMigrator.storeToRedshift(loadedTable._1, loadedTable._2,
                            configuration.redshiftConf, configuration.s3Conf, sqlContext,
                            configuration.internalConfig)

                    }
                val migrationTime: Double = TimeUnit.NANOSECONDS.toMillis(context.stop()) / 1000.0
                logger.info("Successful transfer for configuration\n{}", configuration.toString)
                finalConfigurations :+= configuration.copy(status = Some(Status(isSuccessful = true, null)),
                    migrationTime = Some(migrationTime))
                registerGauge(metricName = s"$metricName.migrationSuccess", value = 1)
            } catch {
                case e: Exception =>
                    logger.error("Transfer Failed for configuration: \n{}", configuration)
                    logger.error("Stack Trace: ", e.fillInStackTrace())
                    finalConfigurations :+= configuration.copy(status = Some(Status(isSuccessful = false, e)),
                        migrationTime = Some(0.0))
                    incCounter(s"$metricName.migrationFailedRetries")
            }
        }
        finalConfigurations
    }

    /**
      * Start table transfers from source to destination(Redshift).
      *
      * @param sqlContext     Spark SQL Context
      * @param configurations List of configurations which needs to be executed
      * @param retryCount     Number of retries
      * @return List of configurations with status and transfer time.
      */
    private def start(sqlContext: SQLContext, configurations: Seq[AppConfiguration], retryCount: Int): Seq[AppConfiguration] = {
        var finalConfiguration = run(sqlContext, configurations)
        (1 to retryCount).foreach { count =>
            if (!Util.anyFailures(finalConfiguration)) {
                logger.info("All failed tables are successfully transferred!!!")
                return finalConfiguration
            } else {
                logger.info("Retrying with count: {}", count)
                Util.exponentialPause(count)
                val fAndS = Util.failedAndSuccessConfigurations(finalConfiguration)
                val rerunConfigurations = run(sqlContext, fAndS._1)
                finalConfiguration = fAndS._2 ++  rerunConfigurations
            }
            incCounter(s"$count.FailedTransferRetry", Util.failedAndSuccessConfigurations(finalConfiguration)._1.size.toLong)
        }
        finalConfiguration
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
        val finalConfigurations = start(sqlContext, configurations, retryCount = appParams.retryCount)
        if (Util.anyFailures(finalConfigurations)) {
            val failedConfigurations = Util.failedAndSuccessConfigurations(finalConfigurations)._1
            logger.info("Failed transfers: {}", failedConfigurations.size)
             val configurationString = Util.createJsonConfiguration(failedConfigurations)
             new PrintWriter("failed.json") {
                 write(configurationString)
                 close()
             }
        }

        //Closing Spark Context
        Util.closeSparkContext(sqlContext.sparkContext)
        logger.info("Info Section: \n{}", Util.formattedInfoSection(finalConfigurations))

        if (appParams.mailDetailsPath == null) {
            logger.info("Mail details properties file is not provided!!!")
            logger.info("Disabling alerting")
        } else if (!appParams.alertOnFailure || Util.anyFailures(finalConfigurations)) {
            logger.info("Alerting developers....")
            val prop: Properties = new Properties()
            prop.load(new File(appParams.mailDetailsPath).toURI.toURL.openStream())
            // Sending mail
            try {
                new MailAPI(MailUtil.getMailParams(prop)).send(finalConfigurations.toList)
            } catch {
                case e: Exception => logger.warn("Failed to send mail with reason: {}", e.getStackTrace.mkString("\n"))
            }
        }

        // For collecting metrics for last table
        Thread.sleep(appParams.metricsWindowSize * 1000)
    }
}