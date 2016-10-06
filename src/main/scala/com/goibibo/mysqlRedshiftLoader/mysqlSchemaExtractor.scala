package com.goibibo.mysqlRedshiftLoader

import java.sql._
import java.util.Properties

import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.{Map, Seq, Set}

/*
--packages "org.apache.hadoop:hadoop-aws:2.7.2,com.databricks:spark-redshift_2.10:1.1.0,com.amazonaws:aws-java-sdk:1.7.4,mysql:mysql-connector-java:5.1.39"
--jars=<Some-location>/RedshiftJDBC4-1.1.17.1017.jar
*/

object mysqlSchemaExtractor {
    private val logger: Logger = LoggerFactory.getLogger(mysqlSchemaExtractor.getClass)

    /**
      * Load table in spark.
      *
      * @param mysqlConfig
      * @param sqlContext
      * @param crashOnInvalidType
      * @return
      */
    def loadToSpark(mysqlConfig: DBConfiguration, sqlContext: SQLContext)
                   (implicit crashOnInvalidType: Boolean):
    (DataFrame, TableDetails) = {
        logger.info("Loading table to Spark from MySQL")
        logger.info("MySQL details: \n{}", mysqlConfig.toString)
        val tableDetails: TableDetails = getValidFieldNames(mysqlConfig)
        logger.info("Table details: \n{}", tableDetails.toString)

        val partitionDetails: Option[Seq[String]] = tableDetails.distributionKey match {
            case Some(primaryKey) =>
                val typeOfPrimaryKey = tableDetails.validFields.filter(_.fieldName == primaryKey).head.fieldType
                //Spark supports only long to break the table into multiple fields
                //https://github.com/apache/spark/blob/branch-1.6/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JDBCRelation.scala#L33
                if (typeOfPrimaryKey.startsWith("INT")) {
                    val sqlQuery =
                        s"""(select min($primaryKey), max($primaryKey)
                                        from ${mysqlConfig.tableName}) AS A"""
                    val dataReader = getDataFrameReader(mysqlConfig, sqlQuery, sqlContext)
                    val data = dataReader.load()
                    val minMaxData = data.rdd.collect()
                    if (minMaxData.length == 1) {
                        val minMaxRow = minMaxData(0)
                        if (minMaxRow(0) != null && minMaxRow(1) != null) {
                            val maxPartitions = 12
                            val predicates = (0 until maxPartitions).toList.map(n => s"($primaryKey mod $maxPartitions) = $n")
                            logger.info(s"$predicates")
                            Some(predicates)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            case None => None
        }

        val columns = tableDetails.validFields.map(_.fieldName).mkString(",")
        //val sqlQuery = s"select ${columns} from ${mysqlConfig.tableName}"
        val dataReader = getDataFrameReader(mysqlConfig, mysqlConfig.tableName, sqlContext)
        val partitionedReader: DataFrame = partitionDetails match {
            case Some(predicates) =>
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
            case None => dataReader.load
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
      * @param df         dataframe
      * @param tableDetails
      * @param redshiftConf
      * @param s3Conf
      * @param sqlContext
      * @param partitions Number of partitions
      */
    def storeToRedshift(df: DataFrame, tableDetails: TableDetails, redshiftConf: DBConfiguration, s3Conf: S3Config,
                        sqlContext: SQLContext)(partitions: Int = 12) = {

        logger.info("Storing to Redshift")
        logger.info("Redshift Details: \n{}", redshiftConf.toString)
        sqlContext.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", s3Conf.accessKey)
        sqlContext.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", s3Conf.secretKey)

        val dropTableString = getDropCommand(redshiftConf)
        val createTableString = getCreateTableString(tableDetails, redshiftConf)
        val preactions = dropTableString + "\n" + createTableString
        val redshiftWriteMode = "append"
        logger.info("Write mode: {}", redshiftWriteMode)

        df.repartition(partitions).write.
                format("com.databricks.spark.redshift").
                option("url", getJdbcUrl(redshiftConf)).
                option("user", redshiftConf.userName).
                option("password", redshiftConf.password).
                option("jdbcdriver", "com.amazon.redshift.jdbc4.Driver").
                option("preactions", preactions).
                option("dbtable", s"${redshiftConf.schema}.${redshiftConf.tableName}").
                option("tempdir", s3Conf.s3Location).
                option("extracopyoptions", "TRUNCATECOLUMNS").
                mode(redshiftWriteMode).
                save()
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

    def getCreateTableString(tableDetails: TableDetails, redshiftConfiguration: DBConfiguration) = {
        val rc = redshiftConfiguration
        val td = tableDetails
        val tableNameWithSchema = if (rc.schema != null && rc.schema != "") s"${rc.schema}.${rc.tableName} " else s"${rc.tableName} "
        val fieldNames = td.validFields.map(r => s"\t${r.fieldName} ${r.fieldType}").mkString(",\n")
        val distributionKey = td.distributionKey match {
            case None => "DISTSTYLE EVEN"
            case Some(key) => s"DISTSTYLE KEY \nDISTKEY ( $key ) "
        }
        val sortKeys = if (td.sortKeys.nonEmpty) "INTERLEAVED SORTKEY ( " + td.sortKeys.mkString(", ") + " )" else ""

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

}
