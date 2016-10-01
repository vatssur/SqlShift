package com.goibibo.mysqlRedshiftLoader

import java.sql._
import java.util.Properties
import scala.collection.immutable.{Seq,Set,Map}
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{ObjectMetadata,PutObjectRequest}
import java.util.regex._

import java.nio.charset.Charset
import java.io._


object mysqlSchemaExtractor {

    //Use this method to get the columns to extract
    //Use sqoop 's --columns option to only request the valid columns
    //Use sqoop 's --map-column-java option to request for the fields that needs typeChange
    def getValidFieldNames(mysqlConfig:DBConfiguration)(implicit crashOnInvalidType:Boolean):TableDetails = {
        val con          = getConnection(mysqlConfig)
        val tableDetails = getTableDetails(con, mysqlConfig)(crashOnInvalidType)
        con.close
        return tableDetails
    }

    def getSqoopCommand(mysqlConfig:DBConfiguration, s3Conf:S3Config, parallalism:Int = 2)(implicit crashOnInvalidType:Boolean) = {
        val tableDetails = getValidFieldNames(mysqlConfig)
        val columns = tableDetails.validFields.map(_.fieldName).mkString(",")
        val customJavMapping = tableDetails.validFields.filter(_.javaType.isDefined).map( d => (d.fieldName, d.javaType.get)).
                                            map(p => s"${p._1}=${p._2}").mkString(",")
        val mappingCommand = if(!customJavMapping.isEmpty) s"--map-column-java ${customJavMapping}" else ""
        val (s3Bucket,s3Key) = getS3BucketAndKey(s3Conf.sqoopedDataKey)
        val s3Location = s"s3a://${s3Conf.accessKey}:${s3Conf.secretKey}@${s3Bucket}/${s3Key}"
        s"""sqoop import -Dfs.s3a.endpoint=s3.ap-south-1.amazonaws.com
            |--options-file options_goibibo.txt
            |--connect jdbc:${mysqlConfig.database}://${mysqlConfig.hostname}:${mysqlConfig.portNo}/${mysqlConfig.db}
            |--table ${mysqlConfig.tableName} 
            |--target-dir ${s3Location} 
            |--as-avrodatafile -m ${parallalism} ${mappingCommand} 
            |--columns ${columns} 
        """.stripMargin.replaceAll("\\s*\\n"," ")
    }

    //This method will drop the table, create TABLE with DIST, SORTKEYS and proper REDSHIFT types
    //Also loads data to the Redshift table from S3
    def uploadDataToRedshift(mysqlConfig:DBConfiguration, redshiftConf:DBConfiguration, s3Conf:S3Config)
        (implicit crashOnInvalidType:Boolean):Unit = {
        val mysqlConnection     = getConnection(mysqlConfig)
        val redshiftConnection  = getConnection(redshiftConf)

        val tableDetails = getTableDetails( mysqlConnection, mysqlConfig )(crashOnInvalidType)
        val createTableString = getCreateTableString( tableDetails, redshiftConf )
        createRedshiftTable(redshiftConnection, redshiftConf, createTableString, true)
        loadS3DataToRedshift(redshiftConnection, s3Conf, redshiftConf, tableDetails) 
        mysqlConnection.close
        redshiftConnection.close()
    }

    def getConnection(conf:DBConfiguration) = {
        val connectionProps = new Properties()
        connectionProps.put("user", conf.userName)
        connectionProps.put("password", conf.password)
        val connectionString = s"jdbc:${conf.database}://${conf.hostname}:${conf.portNo}/${conf.db}"    
        Class.forName("com.mysql.jdbc.Driver")
        Class.forName("com.amazon.redshift.jdbc4.Driver")
        DriverManager.getConnection(connectionString, connectionProps)
    }

    case class RedshiftType(typeName:String, hasPrecision:Boolean = false, hasScale:Boolean = false, precisionMultiplier:Int = 1)

    val mysqlToRedshiftTypeConverter = {
        val maxVarcharSize = 65536
        Map( 
            "TINYINT"               -> RedshiftType("INT2"), 
            "TINYINT UNSIGNED"      -> RedshiftType("INT2"),
            "SMALLINT"              -> RedshiftType("INT2"),
            "SMALLINT UNSIGNED"     -> RedshiftType("INT4"),
            "MEDIUMINT"             -> RedshiftType("INT4"),
            "MEDIUMINT UNSIGNED"    -> RedshiftType("INT4"),
            "INT"                   -> RedshiftType("INT4"),
            "INT UNSIGNED"          -> RedshiftType("INT8"),
            "BIGINT"                -> RedshiftType("INT8"),
            "BIGINT UNSIGNED"       -> RedshiftType("INT8"),//Corner case indeed makes this buggy, Just hoping that it does not occure!
            "FLOAT"                 -> RedshiftType("FLOAT4"),
            "DOUBLE"                -> RedshiftType("FLOAT8"),
            "DECIMAL"               -> RedshiftType("DECIMAL",true,true),
            "CHAR"                  -> RedshiftType("VARCHAR", true, false, 4),
            "VARCHAR"               -> RedshiftType("VARCHAR", true, false, 4),
            "TINYTEXT"              -> RedshiftType("VARCHAR(1024)"),
            "TEXT"                  -> RedshiftType(s"VARCHAR(${maxVarcharSize})"),
            "MEDIUMTEXT"            -> RedshiftType(s"VARCHAR(${maxVarcharSize})"),
            "LONGTEXT"              -> RedshiftType(s"VARCHAR(${maxVarcharSize})"),
            "BOOLEAN"               -> RedshiftType("BOOLEAN"),
            "BOOL"                  -> RedshiftType("BOOLEAN"),
            "ENUM"                  -> RedshiftType("VARCHAR(255)"),
            "SET"                   -> RedshiftType("VARCHAR(255)"),
            "DATE"                  -> RedshiftType("DATE"),
            "TIME"                  -> RedshiftType("VARCHAR(11)"),
            "DATETIME"              -> RedshiftType("TIMESTAMP"),
            "TIMESTAMP"             -> RedshiftType("TIMESTAMP"),
            "YEAR"                  -> RedshiftType("INT")
        )
    }

    def getDistStyleAndKey(con:Connection, setColumns:Set[String], conf:DBConfiguration):Option[String] = {
        val meta = con.getMetaData()
        val resPrimaryKeys  = meta.getPrimaryKeys(conf.db, null, conf.tableName)
        var primaryKeys     = scala.collection.immutable.Set[String]()
        while(resPrimaryKeys.next) {
            val columnName      = resPrimaryKeys.getString(4)
            if( setColumns.contains(columnName.toLowerCase) ) {
                primaryKeys = primaryKeys + columnName  
            } else {
                System.err.println(s"Rejected ${columnName}")
            }
        }
        resPrimaryKeys.close()
        if(primaryKeys.size != 1) None else Some((primaryKeys.toSeq)(0))
    }

    def getIndexes(con:Connection, setColumns:Set[String], conf:DBConfiguration) = {
        val meta = con.getMetaData()
        val resIndexes = meta.getIndexInfo(conf.db, null, conf.tableName, false, false)
        var setIndexedColumns = scala.collection.immutable.Set[String]()
        while(resIndexes.next) {
            val columnName = resIndexes.getString(9)
            if( setColumns.contains(columnName.toLowerCase) ) {
                setIndexedColumns = setIndexedColumns + columnName  
            } else {
                System.err.println(s"Rejected ${columnName}")
            }
        }
        resIndexes.close()
        setIndexedColumns.toIndexedSeq;
    }

    def convertMySqlTypeToRedshiftType(columnType:String, precision:Int, scale:Int) =  {
        val redshiftType =  if(columnType.toUpperCase == "TINYINT" && precision == 1) RedshiftType("BOOLEAN")
                            else mysqlToRedshiftTypeConverter(columnType.toUpperCase)
        //typeName:String, hasPrecision:Boolean = false, hasScale:Boolean = false, precisionMultiplier:Int
        if(redshiftType.hasPrecision && redshiftType.hasScale) {
            s"${redshiftType.typeName}( ${precision * redshiftType.precisionMultiplier}, ${scale} )"
        } else if( redshiftType.hasPrecision ){
            s"${redshiftType.typeName}( ${precision * redshiftType.precisionMultiplier} )"
        } else {
            redshiftType.typeName
        }
    }

    def getTableDetails(con:Connection, conf:DBConfiguration)
                        (implicit crashOnInvalidType:Boolean):TableDetails = {
        val stmt            = con.createStatement()
        val query           = s"SELECT * from ${conf.db}.${conf.tableName} where 1 < 0"
        val rs              = stmt.executeQuery(query)
        val rsmd            = rs.getMetaData()
        val validFieldTypes = mysqlToRedshiftTypeConverter.keys.toSet
        var validFields     = Seq[DBField]()
        var invalidFields   = Seq[DBField]()

        var setColumns = scala.collection.immutable.Set[String]()
        
        for( i <- 1 to rsmd.getColumnCount() ) {
            val columnType = rsmd.getColumnTypeName(i)
            val precision  = rsmd.getPrecision(i)
            val scale      = rsmd.getScale(i)

            if( validFieldTypes.contains(columnType.toUpperCase) ) {
                val redshiftColumnType = convertMySqlTypeToRedshiftType(columnType, precision, scale)
                val javaTypeMapping    = {
                    if(redshiftColumnType == "TIMESTAMP" || redshiftColumnType == "DATE") Some("String")
                    else None
                }
                validFields = validFields :+ DBField(rsmd.getColumnName(i), redshiftColumnType, javaTypeMapping)

            } else {
                if(crashOnInvalidType)
                    throw new IllegalArgumentException(s"Invalid type ${columnType}");
                invalidFields = invalidFields :+ DBField(rsmd.getColumnName(i), columnType)
            }
            setColumns = setColumns + rsmd.getColumnName(i).toLowerCase
            print(s"${rsmd.getColumnName(i)} ${rsmd.getColumnTypeName(i)} ${rsmd.getPrecision(i)} ${rsmd.getScale(i)}\n")
        }
        rs.close()
        stmt.close()
        val sortKeys = getIndexes(con, setColumns, conf)
        val distKey = getDistStyleAndKey(con, setColumns, conf)
        TableDetails( validFields, invalidFields, sortKeys, distKey )
    }

    def getCreateTableString(tableDetails:TableDetails, redshiftConfiguration:DBConfiguration) = {
        val rc = redshiftConfiguration
        val td = tableDetails
        val tableNameWithSchema = if(rc.schema != null && rc.schema != "") s"${rc.schema}.${rc.tableName} " else s"${rc.tableName} "
        val fieldNames = td.validFields.map( r => s"\t${r.fieldName} ${r.fieldType}").mkString(",\n")
        val distributionKey = td.distributionKey match {
            case None => "DISTSTYLE EVEN"
            case Some(key) => s"DISTSTYLE KEY \nDISTKEY ( ${key} ) "
        }
        val sortKeys = if(td.sortKeys.size > 0) "INTERLEAVED SORTKEY ( " + td.sortKeys.mkString(", ") + " )" else ""

        s"""CREATE TABLE IF NOT EXISTS ${tableNameWithSchema} ( 
        |    ${fieldNames}
        |)
        |${distributionKey}
        |${sortKeys} ;""".stripMargin
    }

    def getDropCommand(conf:DBConfiguration) = {
        val tableNameWithSchema = if(conf.schema != null && conf.schema != "" ) s"${conf.schema}.${conf.tableName}"
        else conf.tableName
        s"DROP TABLE IF EXISTS ${tableNameWithSchema};"
    }

    def createRedshiftTable( con:Connection, conf:DBConfiguration, createTableQuery:String, overwrite:Boolean = true) = {
        val stmt            = con.createStatement()
        if(overwrite ) { 
            stmt.executeUpdate(getDropCommand(conf))
        }
        stmt.executeUpdate(createTableQuery)
        stmt.close()
    }

    def makeRedshiftloadCommand(s3Config:S3Config, conf:DBConfiguration, jsonPathFileLocation:String) = {
        val tableNameWithSchema = if(conf.schema != null && conf.schema != "" ) s"${conf.schema}.${conf.tableName}"
        val s3Location = s"${s3Config.sqoopedDataKey}/".replaceAll("/+$","/")
        s"""COPY ${tableNameWithSchema} FROM '${s3Location}' 
        CREDENTIALS 'aws_access_key_id=${s3Config.accessKey};aws_secret_access_key=${s3Config.secretKey}'
        FORMAT AS AVRO  '${jsonPathFileLocation}'
        TRUNCATECOLUMNS; """
    }

    def loadS3DataToRedshift( con:Connection, s3Config:S3Config, conf:DBConfiguration, tableDetails:TableDetails) = {
        val stmt                    = con.createStatement()
        val jsonPathData            = makeJsonPath(tableDetails)
        val jsonPathFileLocation    = writeJsonpathToS3(s3Config, jsonPathData, s"${conf.schema}.${conf.tableName}.txt")
        stmt.executeUpdate(makeRedshiftloadCommand(s3Config,conf, jsonPathFileLocation))
        stmt.close()
    }

    //"$['id']",
    def makeJsonPath(tableDetails:TableDetails):String = {
        val fields = tableDetails.validFields.map(d =>  "\"$['" + d.fieldName + "']\"").mkString(",\n\t")
        s"""{
        |    "jsonpaths": [
        |        ${fields}
        |    ]
        |}""".stripMargin
    }
    def getS3BucketAndKey(s3Location:String):(String, String) = {
        val p = Pattern.compile("s3://([\\w\\-\\._]+)/(.+)")
        val m = p.matcher(s3Location)
        if(!m.matches) {
            throw new IllegalArgumentException(s"Invalid s3 key ${s3Location}, Couldn't find bucket and key")
        } else {
            val bucket = m.group(1)
            val key = m.group(2)
            (bucket, key)
        }
    }
    
    def writeJsonpathToS3(s3Conf:S3Config, data:String, fileName:String):String = {
        val creds = new BasicAWSCredentials(s3Conf.accessKey, s3Conf.secretKey)
        val s3Client = new AmazonS3Client(creds)
        s3Client.setEndpoint("s3.ap-south-1.amazonaws.com")
        val UTF8_CHARSET = Charset.forName("UTF-8");
        val bytes = data.getBytes(UTF8_CHARSET)
        val fileInputStream = new ByteArrayInputStream(bytes)
        val metadata = new ObjectMetadata
        metadata.setContentType("application/json")
        metadata.setContentLength(bytes.length.toLong)
        val (bucket,key) = getS3BucketAndKey(s3Conf.sqoopedDataKey)
        val jsonPathResult = s"sqoop/jsonpathTemp/${fileName}".replaceAll("//","/")
        val putObjectRequest = new PutObjectRequest(bucket, jsonPathResult, fileInputStream, metadata)
        s3Client.putObject(putObjectRequest)
        val outputLocation = s"s3://${bucket}/${jsonPathResult}"
        println(s"Wrote JSONPATH at ${outputLocation}")
        return outputLocation
    }
}


