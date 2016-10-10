package com.goibibo.mysqlRedshiftLoader

import java.io.{File, InputStream}
import java.sql.ResultSet

import com.goibibo.mysqlRedshiftLoader
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s._
import org.json4s.native.JsonMethods._
import org.slf4j.{Logger, LoggerFactory}
import scopt.OptionParser

/**
  * Entry point to RDS to Redshift data pipeline
  */
object Main {
    val KB: Long = 1024
    val MB: Long = KB * KB
    val GB: Long = KB * MB

    private val logger: Logger = LoggerFactory.getLogger(Main.getClass)
    private val parser: OptionParser[AppParams] =
        new OptionParser[AppParams]("Main") {
            head("RDS to Redshift DataPipeline")
            opt[String]("tableDetails")
                    .abbr("td")
                    .text("Table details json file path including")
                    .action((x, c) => c.copy(tableDetailsPath = x))

            opt[Boolean]("targetRecordOverwrite")
                    .abbr("ro")
                    .text("To overwrite updated records in redshift")
                    .action((x, c) => c.copy(targetRecordOverwrite = x))

            opt[String]("targetRecordOverwriteKey")
                    .abbr("rok")
                    .text("To overwrite updated records on particular keys in redshift")
                    .action((x, c) => c.copy(targetRecordOverwriteKey = Option(x)))

            help("help")
                    .text("Usage Of arguments")
        }

    private def getExecutorMemory: Long = {
        val executorMemoryAsString: String = System.getProperty("spark.executor.memory")
        var executorMemory = 512 * MB
        try {
            if (executorMemoryAsString != null) {
                logger.info("Executor Memory: {}", executorMemoryAsString)
                val len: Int = executorMemoryAsString.length
                val memChar: Char = executorMemoryAsString.charAt(len - 1)
                if (memChar.toLower == 'k') {
                    executorMemory = executorMemoryAsString.substring(0, len - 1).toLong * KB
                } else if (memChar.toLower == 'm') {
                    executorMemory = executorMemoryAsString.substring(0, len - 1).toLong * MB
                } else if (memChar.toLower == 'g') {
                    executorMemory = executorMemoryAsString.substring(0, len - 1).toLong * GB
                } else {
                    executorMemory = executorMemoryAsString.toLong
                }
            }
        } catch {
            case e: Exception => logger.error("Wrong format of executor memory")
                logger.info("Taking default executor memory: {}", executorMemory)
        }
        logger.info("Executor memory in bytes: {}", executorMemory)
        executorMemory
    }

    private def getAvgRowSize(mysqlDBConf: DBConfiguration): Long = {
        logger.info("Calculating average row size: {}", mysqlDBConf.toString)
        val query = s"SELECT avg_row_length FROM information_schema.tables WHERE table_schema = " +
                s"'${mysqlDBConf.db}' AND table_name = '${mysqlDBConf.tableName}'"
        val connection = mysqlSchemaExtractor.getConnection(mysqlDBConf)
        val result: ResultSet = connection.createStatement().executeQuery(query)
        result.next()
        val avgRowSize: Long = result.getLong(1)
        connection.close()
        avgRowSize
    }

    def getMinMaxAndRows(mysqlDBConf: DBConfiguration, whereCondition: Option[String] = None): (Long, Long, Long) = {
        val connection = mysqlSchemaExtractor.getConnection(mysqlDBConf)
        val keys: ResultSet = connection.getMetaData.getPrimaryKeys(mysqlDBConf.db, null, mysqlDBConf.tableName)
        keys.next()
        val primaryKey: String = keys.getString(4)
        logger.info("Primary key is: {}", primaryKey)
        var query = s"SELECT min($primaryKey), max($primaryKey), count(*) " +
                s"FROM ${mysqlDBConf.db}.${mysqlDBConf.tableName}"
        if (whereCondition.nonEmpty) {
            query += " WHERE " + whereCondition.get
        }
        logger.info("Running Query: \n{}", query)
        val result: ResultSet = connection.createStatement().executeQuery(query)
        result.next()
        val min: Long = result.getLong(1)
        val max: Long = result.getLong(2)
        val rows: Long = result.getLong(3)
        logger.info(s"Minimum $primaryKey: $min :: Maximum $primaryKey: $max :: Total number Of Rows: $rows")
        connection.close()
        (min, max, rows)
    }

    private def getPartitions(mysqlDBConf: DBConfiguration, whereCondition: Option[String] = None): Int = {
        val memory: Long = getExecutorMemory

        logger.info("Calculating number of partitions with each executor has memory: {}", memory)

        val minMaxAndRows: (Long, Long, Long) = getMinMaxAndRows(mysqlDBConf, whereCondition)
        val minMaxDiff: Long = minMaxAndRows._2 - minMaxAndRows._1 + 1
        val avgRowSize: Long = getAvgRowSize(mysqlDBConf)
        if(avgRowSize == 0) {
            return 0
        }
        logger.info("Average Row size: {}, difference b/w min-max primary key: {}", avgRowSize, minMaxDiff)
        val expectedNumberOfRows = (memory / avgRowSize).toDouble
        logger.info("Expected number of rows: {}", expectedNumberOfRows)

        if(expectedNumberOfRows == 0) {
            return 0
        }
        val partitions: Int = Math.ceil(minMaxDiff / expectedNumberOfRows).toInt
        logger.info("Total number partitions are {}", partitions)
        partitions
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

    private def getAppConfigurations(jsonPath: String, appParams: AppParams): Seq[AppConfiguration] = {
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
                    val incrementalColumn: JValue = table \ "incremental"
                    var internalConfig: InternalConfig = null
                    if (incrementalColumn == JNull) {
                        val partitions: Int = getPartitions(mysqlConf)
                        logger.info("Table is not incremental")
                        internalConfig = InternalConfig(mapPartitions = partitions, reducePartitions = partitions)
                    } else {
                        val whereCondition: String = incrementalColumn.extract[String]
                        val partitions: Int = getPartitions(mysqlConf, Some(whereCondition))
                        logger.info("Table is incremental with condition: {}", whereCondition)
                        val settings: Some[IncrementalSettings] = Some(IncrementalSettings(whereCondition,
                            shallMerge = appParams.targetRecordOverwrite, mergeKey = appParams.targetRecordOverwriteKey,
                            shallVaccumAfterLoad = true))
                        internalConfig = InternalConfig(Some(true), None, incrementalSettings = settings,
                            mapPartitions = partitions, reducePartitions = partitions)
                    }
                    configurations = configurations :+ AppConfiguration(mysqlConf, redshiftConf, s3Conf, internalConfig)
                }
            }
        } finally {
            jsonInputStream.close()
        }
        configurations
    }

    def getSparkContext: (SparkContext, SQLContext) = {
        logger.info("Starting spark context...")
        val sparkConf: SparkConf = new SparkConf().setAppName("RDS to Redshift DataPipeline")
        val sc: SparkContext = new SparkContext(sparkConf)
        val sqlContext: SQLContext = new SQLContext(sc)

        System.setProperty("com.amazonaws.services.s3.enableV4", "true")
        sc.hadoopConfiguration.set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
        (sc, sqlContext)
    }

    private def run(sqlContext: SQLContext, configurations: Seq[AppConfiguration]): Unit = {
        implicit val crashOnInvalidValue: Boolean = true

        for (configuration <- configurations) {
            logger.info("Configuration: \n{}", configuration.toString)
            try {
                val loadedTable: (DataFrame, TableDetails) = mysqlSchemaExtractor.loadToSpark(configuration.mysqlConf,
                    sqlContext, configuration.internalConfig)
                if (loadedTable._1 == null) {
                    throw new Exception("Data frame is null.")
                }
                mysqlSchemaExtractor.storeToRedshift(loadedTable._1, loadedTable._2, configuration.redshiftConf,
                    configuration.s3Conf, sqlContext, configuration.internalConfig)
                logger.info("Successful transfer for configuration\n{}", configuration.toString)
            } catch {
                case e: Exception =>
                    logger.info("Transfer Failed for configuration: \n{}", configuration)
                    logger.error("Stack Trace: ", e.fillInStackTrace())
            }
        }
    }

    def main(args: Array[String]): Unit = {
        logger.info("Reading Arguments")
        val appParams: AppParams = parser.parse(args, AppParams(null)).orNull
        if (appParams.tableDetailsPath == null) {
            logger.error("Table details is not provided is null!!!")
            throw new NullPointerException("Table details is not provided!!!")
        }
        val (_, sqlContext) = getSparkContext

        logger.info("Getting all configurations")
        val configurations: Seq[AppConfiguration] = getAppConfigurations(appParams.tableDetailsPath, appParams)

        logger.info("Total number of tables to transfer are : {}", configurations.length)

        run(sqlContext, configurations)
    }
}