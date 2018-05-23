package com.goibibo.sqlshift

import java.sql.{Connection, DriverManager}
import java.util.Properties

import com.databricks.spark.redshift.RedshiftReaderM
import com.goibibo.sqlshift.MySQLUtil.{getMySQLConnection, logger}
import com.typesafe.config.Config
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source

/**
  * Project: sqlshift
  * Author: shivamsharma
  * Date: 5/10/18.
  */
trait SparkNRedshiftUtil {
    private val logger: Logger = LoggerFactory.getLogger(this.getClass)

    private def getRedshiftConnection(config: Config): Connection = {
        val mysql = config.getConfig("redshift")
        val connectionProps = new Properties()
        connectionProps.put("user", mysql.getString("username"))
        connectionProps.put("password", mysql.getString("password"))
        val jdbcUrl = s"jdbc:redshift://${mysql.getString("hostname")}:${mysql.getInt("portno")}/${mysql.getString("database")}?useSSL=false"
        Class.forName("com.amazon.redshift.jdbc4.Driver")
        DriverManager.getConnection(jdbcUrl, connectionProps)
    }

    def getSparkContext: (SparkContext, SQLContext) = {
        val sparkConf: SparkConf = new SparkConf().setAppName("Full Dump Testing").setMaster("local")
        val sc: SparkContext = new SparkContext(sparkConf)
        val sqlContext: SQLContext = new SQLContext(sc)

        System.setProperty("com.amazonaws.services.s3.enableV4", "true")
        sc.hadoopConfiguration.set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
        sc.hadoopConfiguration.set("fs.s3a.fast.upload", "true")
        (sc, sqlContext)
    }

    lazy val (sc, sqlContext) = getSparkContext

    def readTableFromRedshift(config: Config, tableName: String): DataFrame = {
        val redshift: Config = config.getConfig("redshift")
        val options = Map("dbtable" -> tableName,
            "user" -> redshift.getString("username"),
            "password" -> redshift.getString("password"),
            "url" -> s"jdbc:redshift://${redshift.getString("hostname")}:${redshift.getInt("portno")}/${redshift.getString("database")}",
            "tempdir" -> config.getString("s3.location"),
            "aws_iam_role" -> config.getString("redshift.iamRole")
        )
        RedshiftReaderM.getDataFrameForConfig(options, sc, sqlContext)
    }

    def dropTableRedshift(config: Config, tables: String*): Unit ={
        logger.info("Droping table: {}", tables)
        val conn = getRedshiftConnection(config)
        val statement = conn.createStatement()
        try {
            val dropTableQuery = s"""DROP TABLE ${tables.mkString(",")}"""
            logger.info("Running query: {}", dropTableQuery)
            statement.executeUpdate(dropTableQuery)
        } finally {
            statement.close()
            conn.close()
        }
    }
}
