package com.goibibo.sqlshift

import java.io.File
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.goibibo.sqlshift.alerting.{MailAPI, MailUtil}
import com.goibibo.sqlshift.commons.MetricsWrapper._
import com.goibibo.sqlshift.commons.{MySQLToRedshiftMigrator, Util}
import com.goibibo.sqlshift.models.Configurations.{AppConfiguration, Offset, PAppConfiguration}
import com.goibibo.sqlshift.models.InternalConfs._
import com.goibibo.sqlshift.models.Params.AppParams
import com.goibibo.sqlshift.models.{Configurations, Status}
import com.goibibo.sqlshift.offsetmanagement.OffsetManager
import com.goibibo.sqlshift.offsetmanagement.zookeeper.ZKOffsetManager
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.{Logger, LoggerFactory}
import scopt.OptionParser

import scala.collection.JavaConverters._

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

    def getOffsetManager(pAppConfiguration: PAppConfiguration, tableName: String): Option[OffsetManager] = {
        if (pAppConfiguration.offsetManager.isDefined) {
            val offsetMangerConf: Configurations.OffsetManagerConf = pAppConfiguration.offsetManager.get
            if (offsetMangerConf.`type`.isDefined && offsetMangerConf.`type`.get == "zookeeper") {
                if (offsetMangerConf.prop.isDefined && offsetMangerConf.prop.get.contains("zkquoram")
                        && offsetMangerConf.prop.get.contains("path")) {
                    logger.info("Initiating zookeeper offset manager...")
                    val prop = new Properties()
                    prop.putAll(offsetMangerConf.prop.get.asJava)
                    Some(new ZKOffsetManager(prop, tableName))
                }
                else {
                    logger.warn("Required details are not present. zkquoram & path are mandatory for zookeeper " +
                            "offset manager in prop.")
                    None
                }
            } else if (offsetMangerConf.`class`.isDefined) {
                val prop = new Properties()
                prop.putAll(offsetMangerConf.prop.getOrElse(Map[String, String]()).asJava)
                val args = Array[AnyRef](prop, tableName)
                val offsetManager = Class.forName(offsetMangerConf.`class`.get)
                        .getConstructor(classOf[Properties], classOf[String])
                        .newInstance(args: _*).asInstanceOf[OffsetManager]
                Some(offsetManager)
            } else {
                logger.warn("No field is found to Initiate Offset Manager. Please add type or class...")
                None
            }
        } else {
            logger.info("No Offset Manager is defined...")
            None
        }
    }

    /**
      * Transfer tables provided in configurations and return list of configurations with status and transfer time.
      *
      * @param sqlContext        Spark sql context
      * @param pAppConfiguration List of configurations which needs to be executed
      * @param isReRun           run only failed configurations
      * @return List of configurations with status and transfer time.
      */
    def run(sqlContext: SQLContext, pAppConfiguration: PAppConfiguration, isReRun: Boolean = false): PAppConfiguration = {
        implicit val crashOnInvalidValue: Boolean = true
        var finalConfigurations: Seq[AppConfiguration] = Seq[AppConfiguration]()

        pAppConfiguration.configuration.filter { p =>
            // Checking condition if it is rerun & Failed/Empty then reprocess
            val runCondition = !isReRun || p.status.isEmpty || !p.status.get.isSuccessful
            if(!runCondition)
                finalConfigurations :+= p
            runCondition
        }.foreach { configuration: AppConfiguration =>
            logger.info("Configuration: \n{}", configuration.toString)
            val mySqlTableName = s"${configuration.mysqlConf.db}.${configuration.mysqlConf.tableName}"
            val redshiftTableName = s"${configuration.redshiftConf.schema}.${configuration.mysqlConf.tableName}"

            val offsetManager = getOffsetManager(pAppConfiguration, redshiftTableName)
            val isLockedSuccessful: Option[Boolean] = if (offsetManager.isDefined) {
                logger.info(s"Try taking lock on redshift table: $redshiftTableName")
                Some(offsetManager.get.getLock)
            } else None
            logger.info(s"Locking status for redshift table $redshiftTableName is $isLockedSuccessful")

            if (isLockedSuccessful.isEmpty || isLockedSuccessful.get) {
                val incSettings: Option[IncrementalSettings] = configuration.internalConfig.incrementalSettings

                val internalConfigNew: InternalConfig = if (offsetManager.isDefined && incSettings.isDefined) {
                    val offset: Option[Offset] = offsetManager.get.getOffset
                    val fromOffset = if (incSettings.get.fromOffset.isEmpty && offset.isDefined) offset.get.data else incSettings.get.fromOffset
                    if (incSettings.get.autoIncremental.isDefined && incSettings.get.autoIncremental.get && incSettings.get.incrementalColumn.isDefined) {
                        val (_, max): (String, String) = Util.getMinMax(configuration.mysqlConf, incSettings.get.incrementalColumn.get)
                        configuration.internalConfig.copy(incrementalSettings = Some(incSettings.get.copy(fromOffset = fromOffset, toOffset = Some(max))))
                    } else configuration.internalConfig.copy(incrementalSettings = Some(incSettings.get.copy(fromOffset = fromOffset)))
                } else configuration.internalConfig

                val incSettingsNew: Option[IncrementalSettings] = internalConfigNew.incrementalSettings

                sqlContext.sparkContext.setJobDescription(s"$mySqlTableName => $redshiftTableName")
                val metricName = mySqlTableName + s".${configuration.redshiftConf.schema}"
                try {
                    val context = getTimerMetrics(s"$metricName.migrationMetrics")
                    // Loading table
                    val loadedTable: (DataFrame, TableDetails) = MySQLToRedshiftMigrator.loadToSpark(configuration.mysqlConf,
                        sqlContext, internalConfigNew)
                    if (loadedTable._1 != null) {
                        // Storing table to redshift
                        MySQLToRedshiftMigrator.storeToRedshift(loadedTable._1, loadedTable._2,
                            configuration.redshiftConf, configuration.s3Conf, sqlContext,
                            internalConfigNew)
                    }
                    val migrationTime: Double = TimeUnit.NANOSECONDS.toMillis(context.stop()) / 1000.0
                    logger.info("Successful transfer for configuration\n{}", configuration.toString)
                    finalConfigurations :+= configuration.copy(status = Some(Status(isSuccessful = true, null)),
                        migrationTime = Some(migrationTime))
                    registerGauge(metricName = s"$metricName.migrationSuccess", value = 1)
                    // Setting Offset in OffsetManager
                    if (offsetManager.isDefined && incSettingsNew.isDefined && incSettingsNew.get.toOffset.isDefined) {
                        offsetManager.get.setOffset(Offset(data = incSettingsNew.get.toOffset))
                    }
                } catch {
                    case e: Exception =>
                        logger.error("Transfer Failed for configuration: \n{}", configuration, e: Any)
                        finalConfigurations :+= configuration.copy(status = Some(Status(isSuccessful = false, e)),
                            migrationTime = Some(0.0))
                        incCounter(s"$metricName.migrationFailedRetries")
                } finally {
                    offsetManager.foreach(_.close())
                }
            } else {
                logger.warn(s"Didn't able to take lock on $redshiftTableName")
                finalConfigurations :+= configuration.copy(status = Some(Status(isSuccessful = false, null)),
                    migrationTime = Some(0.0))
            }
        }
        pAppConfiguration.copy(configuration = finalConfigurations)
    }

    /**
      * Start table transfers from source to destination(Redshift).
      *
      * @param sqlContext         Spark SQL Context
      * @param pAppConfigurations List of configurations which needs to be executed
      * @param retryCount         Number of retries
      * @return List of configurations with status and transfer time.
      */
    def start(sqlContext: SQLContext, pAppConfigurations: PAppConfiguration, retryCount: Int): PAppConfiguration = {
        var finalConfiguration = run(sqlContext, pAppConfigurations)
        (1 to retryCount).foreach { count =>
            if (!Util.anyFailures(finalConfiguration)) {
                logger.info("All failed tables are successfully transferred!!!")
                return finalConfiguration
            } else {
                logger.info("Retrying with count: {}", count)
                Util.exponentialPause(count)
                finalConfiguration = run(sqlContext, finalConfiguration, isReRun = true)
            }
            incCounter(s"$count.FailedTransferRetry", Util.failedAndSuccessConfigurations(finalConfiguration.configuration)._1.size.toLong)
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
        val pAppConfigurations = Util.getAppConfigurations(appParams.tableDetailsPath)

        logger.info("Total number of tables to transfer are : {}", pAppConfigurations.configuration.length)

        incCounter("NumberOFTables", pAppConfigurations.configuration.size.toLong)
        val finalConfigurations = start(sqlContext, pAppConfigurations, retryCount = appParams.retryCount)
        if (Util.anyFailures(finalConfigurations)) {
            val failedConfigurations = Util.failedAndSuccessConfigurations(finalConfigurations.configuration)._1
            logger.info("Failed transfers: {}", failedConfigurations.size)
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
                new MailAPI(MailUtil.getMailParams(prop)).send(finalConfigurations.configuration.toList)
            } catch {
                case e: Exception => logger.warn("Failed to send mail with reason", e)
            }
        }

        // For collecting metrics for last table
        Thread.sleep(appParams.metricsWindowSize * 1000)
    }
}
