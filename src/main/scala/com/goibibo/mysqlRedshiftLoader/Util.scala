package com.goibibo.mysqlRedshiftLoader

import java.io.{File, InputStream}
import java.sql.ResultSet

import com.goibibo.mysqlRedshiftLoader
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.native.JsonMethods._
import org.json4s.{DefaultFormats, _}
import org.slf4j.{Logger, LoggerFactory}
import scala.util.{Try, Success, Failure}

/**
  * Project: mysql-redshift-loader
  * Author: shivamsharma
  * Date: 10/11/16.
  */
object Util {
    private val logger: Logger = LoggerFactory.getLogger(Util.getClass)

    val KB: Long = 1024
    val MB: Long = KB * KB
    val GB: Long = KB * MB

    /**
      * Get spark executor memory when provided. Default: 512m
      *
      * @return Executor memory
      */
    //TODO: Replace this function with conf.getSizeAsBytes("spark.executor.memory")
    def getExecutorMemory(conf:SparkConf): Long = {
        val defaultExecutorMemorySize = 512 * MB
        val executorMemorySize  = Try { conf.getSizeAsBytes("spark.executor.memory") }.getOrElse {
            logger.warn("Wrong format of executor memory, Taking default {}", defaultExecutorMemorySize)
            defaultExecutorMemorySize
        }
        logger.info("executorMemorySize = {}", executorMemorySize)
        return executorMemorySize
    }

    /**
      * Get average row size of table.
      *
      * @param mysqlDBConf mysql configuration
      * @return average size of record(row) in bytes
      */
    def getAvgRowSize(mysqlDBConf: DBConfiguration): Long = {
        logger.info("Calculating average row size: {}", mysqlDBConf.toString)
        val query = s"SELECT avg_row_length FROM information_schema.tables WHERE table_schema = " +
                s"'${mysqlDBConf.db}' AND table_name = '${mysqlDBConf.tableName}'"
        val connection = MySqlSchemaExtractor.getConnection(mysqlDBConf)
        val result: ResultSet = connection.createStatement().executeQuery(query)
        result.next()
        val avgRowSize: Long = result.getLong(1)
        result.close()
        connection.close()
        avgRowSize
    }



    /**
      * Get minimum, maximum of primary key if primary key is integer and total records with given where condition
      *
      * @param mysqlDBConf    mysql configuration
      * @param whereCondition filter condition(without where clause)
      * @return tuple: (min, max)
      */
    def getMinMax(mysqlDBConf: DBConfiguration, distKey:String, whereCondition: Option[String] = None): (Long, Long) = {
        val connection = MySqlSchemaExtractor.getConnection(mysqlDBConf)

        var query = s"SELECT min($distKey), max($distKey) " +
                s"FROM ${mysqlDBConf.db}.${mysqlDBConf.tableName}"
        if (whereCondition.nonEmpty) {
            query += " WHERE " + whereCondition.get
        }
        logger.info("Running Query: \n{}", query)
        val result: ResultSet = connection.createStatement().executeQuery(query)
        result.next()
        val min: Long = result.getLong(1)
        val max: Long = result.getLong(2)
        logger.info(s"Minimum $distKey: $min :: Maximum $distKey: $max")
        result.close()
        connection.close()
        return (min, max)
    }

    /**
      * Get optimum number of partitions on the basis of auto incremental and executor size.
      * If fails then return 1
      *
      * @param mysqlDBConf    mysql configuration
      * @param whereCondition filter condition(without where clause)
      * @return no of partitions
      */
    def getPartitions(sqlContext:SQLContext, mysqlDBConf: DBConfiguration, minMaxAndRows:(Long,Long)): Int = {
        val memory: Long = getExecutorMemory(sqlContext.sparkContext.getConf)
        logger.info("Calculating number of partitions with each executor has memory: {}", memory)
        val minMaxDiff: Long = minMaxAndRows._2 - minMaxAndRows._1 + 1
        val avgRowSize: Long = getAvgRowSize(mysqlDBConf)
        if (avgRowSize == 0) {
            return 0
        }
        logger.info("Average Row size: {}, difference b/w min-max primary key: {}", avgRowSize, minMaxDiff)
        val expectedNumberOfRows = (memory / avgRowSize).toDouble
        logger.info("Expected number of rows: {}", expectedNumberOfRows)

        var partitions: Int = Math.ceil(minMaxDiff / expectedNumberOfRows).toInt
        logger.info("Total number partitions are {}", partitions)
        partitions
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

    private def getAppConfiguration(mysqlConf: DBConfiguration, redshiftConf: DBConfiguration, s3Conf: S3Config, table: JValue): AppConfiguration = {
        implicit val formats = DefaultFormats

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

        val distKeyValue = table \ "distkey"
        val distKey: Option[String] = if (distKeyValue != JNothing && distKeyValue != JNull) {
            logger.info("Found distribution key:- {}", distKeyValue.extract[String])
            Some(distKeyValue.extract[String])
        } else {
            logger.info("No distribution key found in configuration")
            None
        }

        val partitionsValue = table \ "partitions"
        var partitions: Option[Int] = {
            if (isSplittable && partitionsValue != JNothing && partitionsValue != JNull) 
                Some(partitionsValue.extract[Int])
            else 
                None
        }
        logger.info("Number of partitions: {}", partitions)

        if (incrementalColumn == JNothing || incrementalColumn == JNull) {
            logger.info("Table is not incremental")
            internalConfig = InternalConfig(shallSplit = Some(isSplittable), distKey = distKey, mapPartitions = partitions,
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
            internalConfig = InternalConfig(shallSplit = Some(isSplittable), distKey = distKey, incrementalSettings = settings,
                mapPartitions = partitions, reducePartitions = partitions)
        }
        AppConfiguration(mysqlConf, redshiftConf, s3Conf, internalConfig)
    }

    def getAppConfigurations(jsonPath: String): Seq[AppConfiguration] = {
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
                    var configuration: AppConfiguration = AppConfiguration(mysqlConf, redshiftConf, s3Conf, null)
                    try {
                        configuration = getAppConfiguration(mysqlConf, redshiftConf, s3Conf, table)
                    } catch {
                        case e: Exception =>
                            configuration.status = Some(Status(isSuccessful = false, e))
                    }
                    configurations = configurations :+ configuration
                    logger.info("\n------------- End :: table: {} -------------", (table \ "name").extract[String])
                }
            }
        } finally {
            jsonInputStream.close()
        }
        configurations
    }

    def anyFailures(appConfigurations: Seq[AppConfiguration]): Boolean = {
        for (appConfiguration <- appConfigurations) {
            if (appConfiguration.status.isEmpty || !appConfiguration.status.get.isSuccessful)
                return true
        }
        false
    }

    def formattedInfoSection(appConfigurations: Seq[AppConfiguration]): String = {
        var formattedString = "-" * 106 + "\n"
        formattedString += String.format("|%4s| %20s| %40s| %20s| %12s|\n", "SNo", "MySQL DB", "Table Name",
            "Redshift Schema", "isSuccessful")
        formattedString += "-" * 106 + "\n"
        var sno = 1
        for (appConf <- appConfigurations) {
            formattedString += String.format("|%4s| %20s| %40s| %20s| %12s|\n", sno.toString, appConf.mysqlConf.db,
                appConf.mysqlConf.tableName, appConf.redshiftConf.schema, appConf.status.get.isSuccessful.toString)
            formattedString += "-" * 106 + "\n"
            sno += 1
        }
        formattedString
    }
}
