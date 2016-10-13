package com.goibibo.mysqlRedshiftLoader

import java.sql._
import java.util.Properties

import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.{Map, Seq, Set}
import java.util.regex._
/*
--packages "org.apache.hadoop:hadoop-aws:2.7.2,com.databricks:spark-redshift_2.10:1.1.0,com.amazonaws:aws-java-sdk:1.7.4,mysql:mysql-connector-java:5.1.39"
--jars=<Some-location>/RedshiftJDBC4-1.1.17.1017.jar
*/

object mysqlSchemaExtractor {
    private val logger: Logger = LoggerFactory.getLogger(mysqlSchemaExtractor.getClass)

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
        val tableDetails: TableDetails = getValidFieldNames(mysqlConfig)
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
                            val minMax: (Long, Long, Long) = Util.getMinMaxAndRows(mysqlConfig, whereCondition)
                            val nr: Long = minMax._2 - minMax._1 + 1

                            val mapPartitions = internalConfig.mapPartitions
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
                    case None => None
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
                        jdbc(getJdbcUrl(mysqlConfig), mysqlConfig.tableName, predicates.toArray, properties)
            case None =>
                val tableQuery = internalConfig.incrementalSettings match {
                    case Some(incrementalSettings) =>
                        val whereCondition = incrementalSettings.whereCondition
                        s"(SELECT * from ${mysqlConfig.tableName} WHERE $whereCondition) AS A"
                    case None => mysqlConfig.tableName
                }
                logger.info("Using single partition read query = {}", tableQuery)
                val dataReader = getDataFrameReader(mysqlConfig, tableQuery, sqlContext)
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
      * @param df dataframe
      * @param tableDetails valid and not valid field details
      * @param redshiftConf redshift configuration
      * @param s3Conf s3 configuration
      * @param sqlContext spark SQL context
      * @param internalConfig Information about incremental and partitions
      */
    def storeToRedshift(df: DataFrame, tableDetails: TableDetails, redshiftConf: DBConfiguration, s3Conf: S3Config,
                        sqlContext: SQLContext, internalConfig: InternalConfig = new InternalConfig) = {

        logger.info("Storing to Redshift")
        logger.info("Redshift Details: \n{}", redshiftConf.toString)
        sqlContext.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", s3Conf.accessKey)
        sqlContext.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", s3Conf.secretKey)
        val redshiftTableName = getTableNameWithSchema(redshiftConf)
        val stagingPrepend = "_staging"
        val redshiftStagingTableName = redshiftTableName + stagingPrepend
        val dropTableString = getDropCommand(redshiftConf)
        logger.info("dropTableString {}", dropTableString)
        val createTableString = getCreateTableString(tableDetails, redshiftConf)
        val redshiftStagingConf = redshiftConf.copy(tableName = redshiftConf.tableName + stagingPrepend)
        val createStagingTableString = getCreateTableString(tableDetails, redshiftStagingConf)
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
            case Some(IncrementalSettings(whereCondition, shallMerge, stagingTableMergeKey, shallVaccumAfterLoad, cs)) =>
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
                        logger.info(s"Shall merge is not specified passing megeKey as empty")
                        ""
                    }
                }
                (dropStatingTableStr, mKey, shallVaccumAfterLoad)
        }

        val preActions = dropAndCreateTableString + (if (dropStagingTableString != "") dropStagingTableString + createStagingTableString else "")
        val postActions: String = if (dropStagingTableString != "") {
            s"""DELETE FROM $redshiftTableName USING $redshiftStagingTableName
                |    WHERE $redshiftTableName.$mergeKey = $redshiftStagingTableName.$mergeKey; """.stripMargin + "\n" + (
                    internalConfig.incrementalSettings.get.customSelectFromStaging match {
                        case None =>
                            s"""INSERT into $redshiftTableName
                                |SELECT * FROM $redshiftStagingTableName;""".stripMargin
                        case Some(customSelect) => {
                            val pattern = Pattern.compile("(?:AS|as)\\s*(\\w+)\\s*(?:,|$)")
                            val matcher = pattern.matcher(customSelect)
                            val customFields = scala.collection.mutable.ListBuffer.empty[String]
                            while(matcher.find()) { 
                                val matched = matcher.group(1); 
                                customFields += matched
                                logger.info("matched =>{}", matched)
                            }
                            val tableColunms = tableDetails.validFields.map(_.fieldName).mkString(",")
                            val customFieldsStr = customFields.mkString(",")
                            val customColumns = if(customFields.size > 0) s"( ${tableColunms},${customFieldsStr} )" else ""
                            logger.info("customColumns =>{}", customColumns)
                            s"""INSERT into $redshiftTableName ${customColumns}
                                |SELECT *,$customSelect FROM $redshiftStagingTableName;""".stripMargin
                        }
                    })
        } else {
            ""
        }

        logger.info("preActions = {}", preActions)
        logger.info("postActions = {}", postActions)

        val redshiftWriteMode = if (dropStagingTableString != "") "overwrite" else "append"
        logger.info("Write mode: {}", redshiftWriteMode)


        val redshiftTableNameForIngestion = if (dropStagingTableString != "") redshiftStagingTableName
        else redshiftTableName

        logger.info("redshiftTableNameForIngestion: {}", redshiftTableNameForIngestion)

        val redshiftWriter = {
            if (df.rdd.getNumPartitions == internalConfig.reducePartitions) df
            else {
                logger.info("Repartitioning the data frame...")
                df.repartition(internalConfig.reducePartitions)
            }
        }.write.
                format("com.databricks.spark.redshift").
                option("url", getJdbcUrl(redshiftConf)).
                option("user", redshiftConf.userName).
                option("password", redshiftConf.password).
                option("jdbcdriver", "com.amazon.redshift.jdbc4.Driver").
                option("dbtable", redshiftTableNameForIngestion).
                option("tempdir", s3Conf.s3Location).
                option("extracopyoptions", "TRUNCATECOLUMNS").
                mode(redshiftWriteMode)

        val redshiftWriterWithPreactions = {
            if (preActions != "") redshiftWriter.option("preactions", preActions)
            else redshiftWriter
        }

        val redshiftWriterWithPostactions = {
            if (postActions != "") redshiftWriterWithPreactions.option("postactions", postActions)
            else redshiftWriterWithPreactions
        }

        redshiftWriterWithPostactions.save()
        if (shallVaccumAfterLoad) {
            performVacuum(redshiftConf)
        } else {
            logger.info("Not opting for Vacuum, shallVaccumAfterLoad is false")
        }
    }


    def getVacuumString(shallVacuumAfterLoad: Boolean, redshiftConf: DBConfiguration) = {
        if (shallVacuumAfterLoad) s"VACUUM DELETE ONLY ${getTableNameWithSchema(redshiftConf)};" else ""
    }

    def getDataFrameReader(mysqlConfig: DBConfiguration, sqlQuery: String, sqlContext: SQLContext): DataFrameReader = {
        sqlContext.read.format("jdbc").
                option("url", getJdbcUrl(mysqlConfig)).
                option("dbtable", sqlQuery).
                option("driver", "com.mysql.jdbc.Driver").
                option("user", mysqlConfig.userName).
                option("password", mysqlConfig.password).
                option("fetchSize", Integer.MIN_VALUE.toString).
                option("fetchsize", Integer.MIN_VALUE.toString) //https://issues.apache.org/jira/browse/SPARK-11474
    }

    //Use this method to get the columns to extract
    //Use sqoop 's --columns option to only request the valid columns
    //Use sqoop 's --map-column-java option to request for the fields that needs typeChange
    def getValidFieldNames(mysqlConfig: DBConfiguration)(implicit crashOnInvalidType: Boolean): TableDetails = {
        val con = getConnection(mysqlConfig)
        val tableDetails = getTableDetails(con, mysqlConfig)(crashOnInvalidType)
        con.close()
        tableDetails
    }

    def getJdbcUrl(conf: DBConfiguration) = {
        val jdbcUrl = s"jdbc:${conf.database}://${conf.hostname}:${conf.portNo}/${conf.db}"
        if (conf.database.toLowerCase == "mysql") jdbcUrl + "?zeroDateTimeBehavior=convertToNull" else jdbcUrl
    }

    def getConnection(conf: DBConfiguration) = {
        val connectionProps = new Properties()
        connectionProps.put("user", conf.userName)
        connectionProps.put("password", conf.password)
        val connectionString = getJdbcUrl(conf)
        Class.forName("com.mysql.jdbc.Driver")
        Class.forName("com.amazon.redshift.jdbc4.Driver")
        DriverManager.getConnection(connectionString, connectionProps)
    }

    case class RedshiftType(typeName: String, hasPrecision: Boolean = false, hasScale: Boolean = false, precisionMultiplier: Int = 1)

    val mysqlToRedshiftTypeConverter = {
        val maxVarcharSize = 65535
        Map(
            "TINYINT" -> RedshiftType("INT2"),
            "TINYINT UNSIGNED" -> RedshiftType("INT2"),
            "SMALLINT" -> RedshiftType("INT2"),
            "SMALLINT UNSIGNED" -> RedshiftType("INT4"),
            "MEDIUMINT" -> RedshiftType("INT4"),
            "MEDIUMINT UNSIGNED" -> RedshiftType("INT4"),
            "INT" -> RedshiftType("INT4"),
            "INT UNSIGNED" -> RedshiftType("INT8"),
            "BIGINT" -> RedshiftType("INT8"),
            "BIGINT UNSIGNED" -> RedshiftType("INT8"), //Corner case indeed makes this buggy, Just hoping that it does not occure!
            "FLOAT" -> RedshiftType("FLOAT4"),
            "DOUBLE" -> RedshiftType("FLOAT8"),
            "DECIMAL" -> RedshiftType("DECIMAL", hasPrecision = true, hasScale = true),
            "CHAR" -> RedshiftType("VARCHAR", hasPrecision = true, hasScale = false, 4),
            "VARCHAR" -> RedshiftType("VARCHAR", hasPrecision = true, hasScale = false, 4),
            "TINYTEXT" -> RedshiftType("VARCHAR(1024)"),
            "TEXT" -> RedshiftType(s"VARCHAR($maxVarcharSize)"),
            "MEDIUMTEXT" -> RedshiftType(s"VARCHAR($maxVarcharSize)"),
            "LONGTEXT" -> RedshiftType(s"VARCHAR($maxVarcharSize)"),
            "BOOLEAN" -> RedshiftType("BOOLEAN"),
            "BOOL" -> RedshiftType("BOOLEAN"),
            "ENUM" -> RedshiftType("VARCHAR(255)"),
            "SET" -> RedshiftType("VARCHAR(255)"),
            "DATE" -> RedshiftType("DATE"),
            "TIME" -> RedshiftType("VARCHAR(11)"),
            "DATETIME" -> RedshiftType("TIMESTAMP"),
            "TIMESTAMP" -> RedshiftType("TIMESTAMP"),
            "YEAR" -> RedshiftType("INT")
        )
    }

    def getDistStyleAndKey(con: Connection, setColumns: Set[String], conf: DBConfiguration): Option[String] = {
        val meta = con.getMetaData
        val resPrimaryKeys = meta.getPrimaryKeys(conf.db, null, conf.tableName)
        var primaryKeys = scala.collection.immutable.Set[String]()
        while (resPrimaryKeys.next) {
            val columnName = resPrimaryKeys.getString(4)
            if (setColumns.contains(columnName.toLowerCase)) {
                primaryKeys = primaryKeys + columnName
            } else {
                System.err.println(s"Rejected $columnName")
            }
        }
        resPrimaryKeys.close()
        if (primaryKeys.size != 1) None else Some(primaryKeys.toSeq.head)
    }

    def getIndexes(con: Connection, setColumns: Set[String], conf: DBConfiguration) = {
        val meta = con.getMetaData
        val resIndexes = meta.getIndexInfo(conf.db, null, conf.tableName, false, false)
        var setIndexedColumns = scala.collection.immutable.Set[String]()
        while (resIndexes.next) {
            val columnName = resIndexes.getString(9)
            if (setColumns.contains(columnName.toLowerCase)) {
                setIndexedColumns = setIndexedColumns + columnName
            } else {
                System.err.println(s"Rejected $columnName")
            }
        }
        resIndexes.close()
        // Redshift can only have 8 interleaved sort keys
        setIndexedColumns.toIndexedSeq.take(8)
    }

    def convertMySqlTypeToRedshiftType(columnType: String, precision: Int, scale: Int) = {
        val redshiftType = if (columnType.toUpperCase == "TINYINT" && precision == 1) RedshiftType("BOOLEAN")
        else mysqlToRedshiftTypeConverter(columnType.toUpperCase)
        //typeName:String, hasPrecision:Boolean = false, hasScale:Boolean = false, precisionMultiplier:Int
        val result = if (redshiftType.hasPrecision && redshiftType.hasScale) {
            s"${redshiftType.typeName}( ${precision * redshiftType.precisionMultiplier}, $scale )"
        } else if (redshiftType.hasPrecision) {
            var redshiftPrecision = precision * redshiftType.precisionMultiplier
            if (redshiftPrecision < 0 || redshiftPrecision > 65535)
                redshiftPrecision = 65535
            s"${redshiftType.typeName}( $redshiftPrecision )"
        } else {
            redshiftType.typeName
        }
        logger.info(s"Converted type: $columnType, precision: $precision, scale:$scale to $result")
        result
    }

    def getTableDetails(con: Connection, conf: DBConfiguration)
                       (implicit crashOnInvalidType: Boolean): TableDetails = {
        val stmt = con.createStatement()
        val query = s"SELECT * from ${conf.db}.${conf.tableName} where 1 < 0"
        val rs = stmt.executeQuery(query)
        val rsmd = rs.getMetaData
        val validFieldTypes = mysqlToRedshiftTypeConverter.keys.toSet
        var validFields = Seq[DBField]()
        var invalidFields = Seq[DBField]()

        var setColumns = scala.collection.immutable.Set[String]()

        for (i <- 1 to rsmd.getColumnCount) {
            val columnType = rsmd.getColumnTypeName(i)
            val precision = rsmd.getPrecision(i)
            val scale = rsmd.getScale(i)

            if (validFieldTypes.contains(columnType.toUpperCase)) {
                val redshiftColumnType = convertMySqlTypeToRedshiftType(columnType, precision, scale)
                val javaTypeMapping = {
                    if (redshiftColumnType == "TIMESTAMP" || redshiftColumnType == "DATE") Some("String")
                    else None
                }
                validFields = validFields :+ DBField(rsmd.getColumnName(i), redshiftColumnType, javaTypeMapping)

            } else {
                if (crashOnInvalidType)
                    throw new IllegalArgumentException(s"Invalid type $columnType")
                invalidFields = invalidFields :+ DBField(rsmd.getColumnName(i), columnType)
            }
            setColumns = setColumns + rsmd.getColumnName(i).toLowerCase
            logger.info(s" column: ${rsmd.getColumnName(i)}, type: ${rsmd.getColumnTypeName(i)}," +
                    s" precision: ${rsmd.getPrecision(i)}, scale:${rsmd.getScale(i)}\n")
        }
        rs.close()
        stmt.close()
        val sortKeys = getIndexes(con, setColumns, conf)
        val distKey = getDistStyleAndKey(con, setColumns, conf)
        TableDetails(validFields, invalidFields, sortKeys, distKey)
    }

    def getTableNameWithSchema(rc: DBConfiguration) = {
        if (rc.schema != null && rc.schema != "") s"${rc.schema}.${rc.tableName}"
        else s"${rc.tableName}"
    }

    def getCreateTableString(td: TableDetails, rc: DBConfiguration) = {
        val tableNameWithSchema = getTableNameWithSchema(rc)
        val fieldNames = td.validFields.map(r => s"""\t"${r.fieldName}" ${r.fieldType}""").mkString(",\n")
        val distributionKey = td.distributionKey match {
            case None => "DISTSTYLE EVEN"
            case Some(key) => s"""DISTSTYLE KEY \nDISTKEY ( "$key" ) """
        }
        val sortKeys = if (td.sortKeys.nonEmpty) "INTERLEAVED SORTKEY ( \"" + td.sortKeys.mkString("\", \"") + "\" )" else ""

        s"""CREATE TABLE IF NOT EXISTS $tableNameWithSchema (
            |    $fieldNames
            |)
            |$distributionKey
            |$sortKeys ;""".stripMargin
    }

    def getDropCommand(conf: DBConfiguration) = {
        val tableNameWithSchema = if (conf.schema != null && conf.schema != "") s"${conf.schema}.${conf.tableName}"
        else conf.tableName
        s"DROP TABLE IF EXISTS $tableNameWithSchema;"
    }

    def createRedshiftTable(con: Connection, conf: DBConfiguration, createTableQuery: String, overwrite: Boolean = true) = {
        val stmt = con.createStatement()
        if (overwrite) {
            stmt.executeUpdate(getDropCommand(conf))
        }
        stmt.executeUpdate(createTableQuery)
        stmt.close()
    }

    def performVacuum(conf: DBConfiguration) = {
        logger.info("Initiating the connection for vacuum")
        val con = getConnection(conf)
        logger.info("creating statement for Connection")
        val stmt = con.createStatement()
        val vacuumString = getVacuumString(shallVacuumAfterLoad = true, conf)
        logger.info("Running command {}", vacuumString)
        stmt.executeUpdate(vacuumString)
        stmt.close()
        con.close()
    }
}
