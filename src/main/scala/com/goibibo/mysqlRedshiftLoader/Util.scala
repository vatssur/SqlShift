package com.goibibo.mysqlRedshiftLoader

import java.sql.ResultSet

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

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
    def getExecutorMemory: Long = {
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
            case e: Exception => logger.warn("Wrong format of executor memory")
                logger.info("Taking default executor memory: {}", executorMemory)
        }
        logger.info("Executor memory in bytes: {}", executorMemory)
        executorMemory
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
        val connection = mysqlSchemaExtractor.getConnection(mysqlDBConf)
        val result: ResultSet = connection.createStatement().executeQuery(query)
        result.next()
        val avgRowSize: Long = result.getLong(1)
        connection.close()
        avgRowSize
    }

    /**
      * Get minimum, maximum of primary key if primary key is integer and total records with given where condition
      *
      * @param mysqlDBConf    mysql configuration
      * @param whereCondition filter condition(without where clause)
      * @return tuple: (min, max, rows)
      */
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

    /**
      * Get optimum number of partitions on the basis of auto incremental and executor size.
      * If fails then return 1
      *
      * @param mysqlDBConf    mysql configuration
      * @param whereCondition filter condition(without where clause)
      * @return no of partitions
      */
    def getPartitions(mysqlDBConf: DBConfiguration, whereCondition: Option[String] = None): Int = {
        val memory: Long = getExecutorMemory
        logger.info("Calculating number of partitions with each executor has memory: {}", memory)
        val minMaxAndRows: (Long, Long, Long) = getMinMaxAndRows(mysqlDBConf, whereCondition)
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

    /**
      * Formatted Status
      * @param appConfigurations
      * @return
      */
    def formattedInfoSection(appConfigurations: Seq[AppConfiguration]): String = {
        var formattedString = "-" * 100 + "\n"
        formattedString += String.format("|%20s| %40s| %20s| 12%s|\n", "MySQL DB", "Table Name", "Redshift Schema",
            "isSuccessful")
        for (appConf <- appConfigurations) {
            formattedString += String.format("|%20s| %40s| %20s| 10%s|\n", appConf.mysqlConf.db,
                appConf.mysqlConf.tableName, appConf.redshiftConf.schema, appConf.status.get.isSuccessful.toString)
        }
        formattedString += "-" * 100 + "\n"
        formattedString
    }
}
