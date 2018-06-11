package com.goibibo.sqlshift.models

/**
  * Project: sqlshift
  * Author: shivamsharma
  * Date: 5/9/18.
  */
case class ApplicationConfiguration(
                                        offsetManager: Option[OffsetManager],
                                        configuration: List[Configuration]
                                )


case class OffsetManager(
                                `type`: Option[String],
                                `class`: Option[String],
                                prop: Option[Map[String, String]]
                        )

case class Configuration(
                                mysql: SQLDatabaseConfiguration,
                                tables: List[TableConfiguration],
                                redshift: RedshiftConfiguration,
                                s3: S3Configuration
                        )

case class SQLDatabaseConfiguration(
                                           db: String,
                                           hostname: String,
                                           portno: Int,
                                           username: String,
                                           password: String
                                   )

case class RedshiftConfiguration(
                                        schema: String,
                                        hostname: String,
                                        portno: Int,
                                        username: String,
                                        password: String
                                )

case class S3Configuration(
                                  location: String,
                                  accessKey: String,
                                  secretKey: String
                          )

case class TableConfiguration(
                                     name: String,
                                     incrementalColumn: Option[String],
                                     isSplittable: Option[String],
                                     mergeKey: Option[String],
                                     fromOffset: Option[String],
                                     toOffset: Option[String],
                                     appendOnly: Option[Boolean],
                                     autoIncremental: Option[Boolean]
                             )