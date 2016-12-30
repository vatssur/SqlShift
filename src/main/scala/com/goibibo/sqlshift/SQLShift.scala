package com.goibibo.sqlshift

import java.io.File
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.Timer.Context
import com.codahale.metrics._
import com.goibibo.sqlshift.alerting.MailUtil
import com.goibibo.sqlshift.commons.{MySQLToRedshiftMigrator, Util}
import com.goibibo.sqlshift.models.Configurations.AppConfiguration
import com.goibibo.sqlshift.models.InternalConfs.TableDetails
import com.goibibo.sqlshift.models.Params.{AppParams, MailParams}
import com.goibibo.sqlshift.models.Status
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.{Logger, LoggerFactory}
import scopt.OptionParser

/**
  * Entry point to RDS to Redshift data pipeline
  */
object SQLShift {
    val registry: MetricRegistry = new MetricRegistry()

    private val slf4jReporter: Slf4jReporter = Slf4jReporter.forRegistry(registry)
            .outputTo(LoggerFactory.getLogger("SQLShiftMetrics"))
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build()

    private val jmxReporter: JmxReporter = JmxReporter.forRegistry(registry).build()

    private val logger: Logger = LoggerFactory.getLogger(SQLShift.getClass)
    private val parser: OptionParser[AppParams] =
        new OptionParser[AppParams]("Main") {
            head("RDS to Redshift DataPipeline")
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
                    .text("Metrics window size. Default: 5 seconds")
                    .optional()
                    .action((x, c) => c.copy(metricsWindowSize = x))

            help("help")
                    .text("Usage Of arguments")
        }

    private def run(sqlContext: SQLContext, configurations: Seq[AppConfiguration], isRetry: Boolean): Unit = {
        implicit val crashOnInvalidValue: Boolean = true

        val counter: Counter = registry.counter("NumberOFTables")
        counter.inc(configurations.size)

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
                    val timer: Timer = registry.timer(s"${configuration.mysqlConf.tableName}-load-metric")
                    val timeContext: Context = timer.time()
                    // Loading table
                    val loadedTable: (DataFrame, TableDetails) = MySQLToRedshiftMigrator.loadToSpark(configuration.mysqlConf,
                        sqlContext, configuration.internalConfig)

                    // Storing table to redshift
                    if (loadedTable._1 != null) {
                        MySQLToRedshiftMigrator.storeToRedshift(loadedTable._1, loadedTable._2, configuration.redshiftConf,
                            configuration.s3Conf, sqlContext, configuration.internalConfig)
                    }
                    timeContext.stop()
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
        val counter: Counter = registry.counter("NumberOFFailedTables")
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
            counter.inc(failedConfigurations.size)
            retryCnt -= 1
        }
        logger.info("Some failed tables are still left")
    }

    private def startMetrics(appParams: AppParams): Unit = {
        // Start metrics reporter
        if (appParams.logMetricsReporting) {
            logger.info("Starting log reporting with window size: {}", appParams.metricsWindowSize)
            slf4jReporter.start(appParams.metricsWindowSize, TimeUnit.SECONDS)
        }

        if (appParams.jmxMetricsReporting) {
            logger.info("Starting JMX reporting...")
            jmxReporter.start()
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

        run(sqlContext, configurations, isRetry = false)
        if (appParams.retryCount > 0 && Util.anyFailures(configurations)) {
            logger.info("Found failed transfers with retry count: {}", appParams.retryCount)
            rerun(sqlContext, configurations, retryCount = appParams.retryCount)
        }

        //Closing Spark Context
        Util.closeSparkContext(sqlContext.sparkContext)

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

        // For collecting metrics for last table
        Thread.sleep(appParams.metricsWindowSize * 1000)

        logger.info("Info Section: \n{}", Util.formattedInfoSection(configurations))
    }
}