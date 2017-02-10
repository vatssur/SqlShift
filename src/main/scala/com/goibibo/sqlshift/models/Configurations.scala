package com.goibibo.sqlshift.models

import com.goibibo.sqlshift.models.InternalConfs.InternalConfig

/**
  * Project: mysql-redshift-loader
  * Author: shivamsharma
  * Date: 12/29/16.
  */
private[sqlshift] sealed trait Configuration

private[sqlshift] object Configurations {

    case class DBConfiguration(database: String,
                               db: String,
                               schema: String,
                               tableName: String,
                               hostname: String,
                               portNo: Int,
                               userName: String,
                               password: String) extends Configuration {

        override def toString: String = {
            s"""{
               |   Database Type: $database,
               |   Database Name: $db,
               |   Table Name: $tableName,
               |   Schema: $schema
               |}""".stripMargin
        }
    }

    case class S3Config(s3Location: String,
                        accessKey: String,
                        secretKey: String) extends Configuration


    case class AppConfiguration(mysqlConf: DBConfiguration,
                                redshiftConf: DBConfiguration,
                                s3Conf: S3Config,
                                internalConfig: InternalConfig,
                                status: Option[Status] = None,
                                migrationTime: Option[Double]= None) {

        override def toString: String = {
            val mysqlString: String = "\tmysql-db : " + mysqlConf.db + "\n\tmysql-table : " + mysqlConf.tableName
            val redshiftString: String = "\tredshift-schema : " + redshiftConf.schema + "\n\tredshift-table : " +
                    redshiftConf.tableName
            "{\n" + mysqlString + "\n" + redshiftString + "\n}"
        }
    }

}