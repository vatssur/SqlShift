package com.goibibo

package object mysqlRedshiftLoader {

    case class DBConfiguration(database: String, db: String, schema: String,
                               tableName: String, hostname: String, portNo: Int, userName: String, password: String) {

        override def toString: String = {
            s"""{
                |   Database Type: $database,
                |   Database Name: $db,
                |   Table Name: $tableName,
                |   Schema: $schema
                |}
                |""".stripMargin
        }
    }

    case class S3Config(s3Location: String, accessKey: String, secretKey: String)

    case class DBField(fieldName: String, fieldType: String, javaType: Option[String] = None) {

        override def toString: String = {
            s"""{
                |   Field Name: $fieldName,
                |   Field Type: $fieldType,
                |   Java Type: $javaType
                |}
                |""".stripMargin
        }
    }

    case class TableDetails(validFields: Seq[DBField], invalidFields: Seq[DBField],
                            sortKeys: Seq[String], distributionKey: Option[String]) {

        override def toString: String = {
            s"""{
                |   Valid Fields: $validFields,
                |   Invaild Fields: $invalidFields,
                |   Interleaved Sort Keys: $sortKeys,
                |   Distribution Keys: $distributionKey
                |}
                |""".stripMargin
        }
    }

    //In the case of IncrementalSettings shallCreateTable should be false by default
    //whereCondition shall not be wrapped with brackets ()
    //Also whereCondition shall not be empty and shall be valid SQL
    case class IncrementalSettings(whereCondition:String, shallDeletePastRecords:Boolean = false, 
                                    shallVaccumAfterLoad:Boolean = false)
    
    //Defaults, 
    //If shallSplit = None then shallSplit = true
    
    //If shallCreateTable = None && incrementalSettings = None
    //    then shallCreateTable is true
    //If shallCreateTable = None && incrementalSettings != None
    //    then shallCreateTable is false
    //If shallCreateTable != None
    //    shallCreateTable = shallCreateTable.get

    case class InternalConfig( shallSplit:Option[Boolean] = None, shallCreateTable:Option[Boolean] = None, 
        incrementalSettings:Option[IncrementalSettings] = None )

    case class AppParams(mysqlConfPath: String, s3ConfPath: String, 
        redshiftConfPath: String, tableDetailsPath: String, )

    case class AppConfiguration(mysqlConf: DBConfiguration, redshiftConf: DBConfiguration, s3Conf: S3Config) {
        override def toString: String = {
            val mysqlString: String = "\tmysql-db : " + mysqlConf.db + "\n\tmysql-table : " + mysqlConf.tableName
            val redshiftString: String = "\tredshift-schema : " + redshiftConf.schema + "\n\tredshift-table : " +
                    redshiftConf.tableName
            "{\n" + mysqlString + "\n" + redshiftString + "\n}"
        }
    }

}

