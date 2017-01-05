package com.goibibo.sqlshift.commons

import java.util.Properties
import java.util.regex._

import com.goibibo.sqlshift.models.Configurations.{DBConfiguration, S3Config}
import com.goibibo.sqlshift.models.InternalConfs.{IncrementalSettings, InternalConfig, TableDetails}
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.Seq

/*
--packages "org.apache.hadoop:hadoop-aws:2.7.2,com.databricks:spark-redshift_2.10:1.1.0,com.amazonaws:aws-java-sdk:1.7.4,mysql:mysql-connector-java:5.1.39"
--jars=<Some-location>/RedshiftJDBC4-1.1.17.1017.jar
*/

object MySQLToRedshiftMigrator {
    private val logger: Logger = LoggerFactory.getLogger(MySQLToRedshiftMigrator.getClass)

    /**
      * Load table in spark.
      *
      * @param mysqlConfig        Mysql connection configuration
      * @param sqlContext         Spark SQLContext
      * @param crashOnInvalidType ToDo: What is this?
      * @return
      */
    def loadToSpark(mysqlConfig: DBConfiguration, sqlContext: SQLContext, internalConfig: InternalConfig = new InternalConfig)
                   (implicit crashOnInvalidType: Boolean): (DataFrame, TableDetails) = {

        logger.info("Loading table to Spark from MySQL")
        logger.info("MySQL details: \n{}", mysqlConfig.toString)
        val tableDetails: TableDetails = RedshiftUtil.getValidFieldNames(mysqlConfig, internalConfig)
        logger.info("Table details: \n{}", tableDetails.toString)

        val partitionDetails: Option[Seq[String]] = internalConfig.shallSplit match {
            case Some(false) =>
                logger.info("shallSplit is false")
                None
            case _ =>
                logger.info("shallSplit either not set or true")
                tableDetails.distributionKey match {
                    case Some(primaryKey) =>
                        val typeOfPrimaryKey = tableDetails.validFields.filter(_.fieldName == primaryKey).head.fieldType
                        //Spark supports only long to break the table into multiple fields
                        //https://github.com/apache/spark/blob/branch-1.6/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JDBCRelation.scala#L33
                        if (typeOfPrimaryKey.startsWith("INT")) {

                            val whereCondition = internalConfig.incrementalSettings match {
                                case Some(incrementalSettings) =>
                                    logger.info("Found where condition {}", incrementalSettings.whereCondition)
                                    Some(incrementalSettings.whereCondition)
                                case None =>
                                    logger.info("Found no where condition ")
                                    None
                            }

                            val minMax: (Long, Long) = Util.getMinMax(mysqlConfig, primaryKey, whereCondition)
                            val nr: Long = minMax._2 - minMax._1 + 1

                            val mapPartitions = internalConfig.mapPartitions match {
                                case Some(partitions) => partitions
                                case None => Util.getPartitions(sqlContext, mysqlConfig, minMax)
                            }
                            if (mapPartitions == 0) {
                                return (null, null)
                            }
                            val inc: Long = Math.ceil(nr.toDouble / mapPartitions).toLong
                            val predicates = (0 until mapPartitions).toList.
                                    map { n =>
                                        s"$primaryKey BETWEEN ${minMax._1 + n * inc} AND ${minMax._1 - 1 + (n + 1) * inc} "
                                    }.
                                    map(c => if (whereCondition.isDefined) c + s"AND (${whereCondition.get})" else c)
                            Some(predicates)
                        } else {
                            logger.warn(s"primary keys is non INT $typeOfPrimaryKey")
                            None
                        }
                    case None =>
                        logger.warn("No Distribution key found!!!")
                        None
                }
        }

        val partitionedReader: DataFrame = partitionDetails match {
            case Some(predicates) =>
                logger.info("Using partitionedRead {}", predicates)
                val properties = new Properties()
                properties.setProperty("user", mysqlConfig.userName)
                properties.setProperty("password", mysqlConfig.password)

                sqlContext.read.
                        option("driver", "com.mysql.jdbc.Driver").
                        option("fetchSize", Integer.MIN_VALUE.toString).
                        option("fetchsize", Integer.MIN_VALUE.toString).
                        option("user", mysqlConfig.userName).
                        option("password", mysqlConfig.password).
                        jdbc(RedshiftUtil.getJDBCUrl(mysqlConfig), mysqlConfig.tableName, predicates.toArray, properties)
            case None =>
                val tableQuery = internalConfig.incrementalSettings match {
                    case Some(incrementalSettings) =>
                        val whereCondition = incrementalSettings.whereCondition
                        s"(SELECT * from ${mysqlConfig.tableName} WHERE $whereCondition) AS A"
                    case None => mysqlConfig.tableName
                }
                logger.info("Using single partition read query = {}", tableQuery)
                val dataReader = RedshiftUtil.getDataFrameReader(mysqlConfig, tableQuery, sqlContext)
                dataReader.load
        }
        val data = partitionedReader.selectExpr(tableDetails.validFields.map(_.fieldName): _*)
        val dataWithTypesFixed = tableDetails.validFields.filter(_.javaType.isDefined).foldLeft(data) {
            (df, dbField) => {
                val modifiedCol = df.col(dbField.fieldName).cast(dbField.javaType.get)
                df.withColumn(dbField.fieldName, modifiedCol)
            }
        }
        logger.info("Table load in spark is finished!!!")
        (dataWithTypesFixed, tableDetails)
    }

    /**
      * Store Dataframe to Redshift table. Drop table if exists.
      * It table doesn't exist it will create table.
      *
      * @param df             dataframe
      * @param tableDetails   valid and not valid field details
      * @param redshiftConf   redshift configuration
      * @param s3Conf         s3 configuration
      * @param sqlContext     spark SQL context
      * @param internalConfig Information about incremental and partitions
      */
    def storeToRedshift(df: DataFrame, tableDetails: TableDetails, redshiftConf: DBConfiguration, s3Conf: S3Config,
                        sqlContext: SQLContext, internalConfig: InternalConfig = new InternalConfig): Unit = {

        logger.info("Storing to Redshift")
        logger.info("Redshift Details: \n{}", redshiftConf.toString)
        sqlContext.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", s3Conf.accessKey)
        sqlContext.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", s3Conf.secretKey)
        val redshiftTableName = RedshiftUtil.getTableNameWithSchema(redshiftConf)
        val stagingPrepend = "_staging"
        val redshiftStagingTableName = redshiftTableName + stagingPrepend
        val dropTableString = RedshiftUtil.getDropCommand(redshiftConf)
        logger.info("dropTableString {}", dropTableString)
        val createTableString = RedshiftUtil.getCreateTableString(tableDetails, redshiftConf)
        //val redshiftStagingConf = redshiftConf.copy(tableName = redshiftConf.tableName + stagingPrepend)
        val createStagingTableString = RedshiftUtil.getCreateTableString(tableDetails, redshiftConf, isStaging = true)
        logger.info("createTableString {}", createTableString)
        val shallOverwrite = internalConfig.shallOverwrite match {
            case None =>
                internalConfig.incrementalSettings match {
                    case None =>
                        logger.info("internalConfig.shallOverwrite is None and internalConfig.incrementalSettings is None")
                        true
                    case Some(_) =>
                        logger.info("internalConfig.shallOverwrite is None and internalConfig.incrementalSettings is Some")
                        false
                }
            case Some(sct) =>
                logger.info("internalConfig.shallOverwrite is {}", sct)
                sct
        }

        val dropAndCreateTableString = if (shallOverwrite) dropTableString + "\n" + createTableString else createTableString

        val (dropStagingTableString: String, mergeKey: String, shallVaccumAfterLoad: Boolean) = internalConfig.incrementalSettings match {
            case None =>
                logger.info("No dropStagingTableString and No vacuum, internalConfig.incrementalSettings is None")
                ("", "", false)
            case Some(IncrementalSettings(whereCondition, shallMerge, stagingTableMergeKey, vaccumAfterLoad, cs)) =>
                val dropStatingTableStr = if (shallMerge) s"DROP TABLE IF EXISTS $redshiftStagingTableName;" else ""
                logger.info(s"dropStatingTableStr = {}", dropStatingTableStr)
                val mKey: String = {
                    if (shallMerge) {
                        stagingTableMergeKey match {
                            case None =>
                                logger.info("mergeKey is also not provided, we use primary key in this case {}",
                                    tableDetails.distributionKey.get)
                                //If primaryKey is not available and mergeKey is also not provided
                                //This means wrong input, get will crash if tableDetails.distributionKey is None
                                tableDetails.distributionKey.get
                            case Some(mk) =>
                                logger.info(s"Found mergeKey = {}", mk)
                                mk
                        }
                    } else {
                        logger.info(s"Shall merge is not specified passing mergeKey as empty")
                        ""
                    }
                }
                (dropStatingTableStr, mKey, vaccumAfterLoad)
        }

        val preActions: String = dropAndCreateTableString +
                (if (dropStagingTableString != "")
                    dropStagingTableString + alterTableQuery(tableDetails, redshiftConf) + "\n" +
                            createStagingTableString
                else "")

        val postActions: String = if (dropStagingTableString != "") {
            val tableColumns = "\"" + tableDetails.validFields.map(_.fieldName).mkString("\", \"") + "\""

            s"""DELETE FROM $redshiftTableName USING $redshiftStagingTableName
               |    WHERE $redshiftTableName.$mergeKey = $redshiftStagingTableName.$mergeKey; """.stripMargin + "\n" +
                    (internalConfig.incrementalSettings.get.customSelectFromStaging match {
                        case None =>
                            // Handling columns order mismatch
                            s"""INSERT INTO $redshiftTableName ($tableColumns)
                               |SELECT $tableColumns FROM $redshiftStagingTableName;""".stripMargin
                        case Some(customSelect) =>
                            val pattern = Pattern.compile("(?:AS|as)\\s*(\\w+)\\s*(?:,|$)")
                            val matcher = pattern.matcher(customSelect)
                            val customFields = scala.collection.mutable.ListBuffer.empty[String]
                            while (matcher.find()) {
                                val matched = matcher.group(1)
                                customFields += matched
                                logger.info("matched => {}", matched)
                            }
                            val customFieldsStr = "\"" + customFields.mkString("\", \"") + "\""
                            val customColumns = if (customFields.nonEmpty) s"( $tableColumns, $customFieldsStr )" else ""
                            logger.info("customColumns => {}", customColumns)
                            s"""INSERT INTO $redshiftTableName $customColumns
                               |SELECT *, $customFieldsStr FROM $redshiftStagingTableName;""".stripMargin
                    }) + "\n" + dropStagingTableString
        } else {
            ""
        }

        logger.info("Redshift PreActions = {}", preActions)
        logger.info("Redshift PostActions = {}", postActions)

        val redshiftWriteMode = if (dropStagingTableString != "") "overwrite" else "append"
        logger.info("Redshift write mode: {}", redshiftWriteMode)


        val redshiftTableNameForIngestion = if (dropStagingTableString != "") redshiftStagingTableName
        else redshiftTableName

        logger.info("redshiftTableNameForIngestion: {}", redshiftTableNameForIngestion)

        val redshiftWriterPartitioned: DataFrame = internalConfig.reducePartitions match {
            case Some(reducePartitions) =>
                if (df.rdd.getNumPartitions == reducePartitions)
                    df.repartition(reducePartitions)
                else
                    df
            case None => df
        }

        val redshiftWriter = redshiftWriterPartitioned.write.
                format("com.databricks.spark.redshift").
                option("url", RedshiftUtil.getJDBCUrl(redshiftConf)).
                option("user", redshiftConf.userName).
                option("password", redshiftConf.password).
                option("jdbcdriver", "com.amazon.redshift.jdbc4.Driver").
                option("dbtable", redshiftTableNameForIngestion).
                option("tempdir", s3Conf.s3Location).
                option("extracopyoptions", "TRUNCATECOLUMNS").
                mode(redshiftWriteMode)

        val redshiftWriterWithPreActions = {
            if (preActions != "") redshiftWriter.option("preactions", preActions)
            else redshiftWriter
        }

        val redshiftWriterWithPostActions = {
            if (postActions != "") redshiftWriterWithPreActions.option("postactions", postActions)
            else redshiftWriterWithPreActions
        }

        redshiftWriterWithPostActions.save()
        try {
            if (shallVaccumAfterLoad) {
                RedshiftUtil.performVacuum(redshiftConf)
            } else {
                logger.info("Not opting for Vacuum, shallVacuumAfterLoad is false")
            }
        } catch {
            case e: Exception => logger.warn("Vacuum failed for reason: {}", e.getStackTrace.mkString("\n"))
        }
    }

    /**
      * Alter table to add or delete columns in redshift table if any changes occurs in sql table
      *
      * @param tableDetails sql table details
      * @param redshiftConf redshift configuration
      * @return Query of add and delete columns from redshift table
      */
    private def alterTableQuery(tableDetails: TableDetails, redshiftConf: DBConfiguration): String = {

        val redshiftTableName: String = RedshiftUtil.getTableNameWithSchema(redshiftConf)
        try {
            val mainTableColumnNames: Set[String] = RedshiftUtil.getColumnNamesAndTypes(redshiftConf).keys.toSet

            // All columns name must be distinct other wise redshift load will fail
            val stagingTableColumnAndTypes: Map[String, String] = tableDetails
                    .validFields
                    .map { td => td.fieldName.toLowerCase -> td.fieldType }
                    .toMap

            val stagingTableColumnNames: Set[String] = stagingTableColumnAndTypes.keys.toSet
            val addedColumns: Set[String] = stagingTableColumnNames -- mainTableColumnNames
            val deletedColumns: Set[String] = mainTableColumnNames -- stagingTableColumnNames

            val addColumnsQuery = addedColumns.foldLeft("\n") { (query, columnName) =>
                query + s"""ALTER TABLE $redshiftTableName ADD COLUMN "$columnName" """ +
                        stagingTableColumnAndTypes.getOrElse(columnName, "") + ";\n"
            }

            val deleteColumnQuery = deletedColumns.foldLeft("\n") { (query, columnName) =>
                query + s"""ALTER TABLE $redshiftTableName DROP COLUMN "$columnName" ;\n"""
            }

            addColumnsQuery + deleteColumnQuery
        } catch {
            case e: Exception =>
                logger.warn("Error occurred while altering table: \n{}", e.getStackTrace.mkString("\n"))
                ""
        }
    }
}
