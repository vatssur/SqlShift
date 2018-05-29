package com.goibibo.sqlshift

import java.sql.{Connection, DriverManager}
import java.util.Properties

import com.databricks.spark.redshift.RedshiftReaderM
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Project: sqlshift
  * Author: shivamsharma
  * Date: 5/10/18.
  */
trait SparkNRedshiftUtil extends BeforeAndAfterAll {
    self: Suite =>
    private val logger: Logger = LoggerFactory.getLogger(this.getClass)
    @transient private var _sc: SparkContext = _
    @transient private var _sqlContext: SQLContext = _

    def sc: SparkContext = _sc
    def sqlContext: SQLContext = _sqlContext

    private def getRedshiftConnection(config: Config): Connection = {
        val mysql = config.getConfig("redshift")
        val connectionProps = new Properties()
        connectionProps.put("user", mysql.getString("username"))
        connectionProps.put("password", mysql.getString("password"))
        val jdbcUrl = s"jdbc:redshift://${mysql.getString("hostname")}:${mysql.getInt("portno")}/${mysql.getString("database")}?useSSL=false"
        Class.forName("com.amazon.redshift.jdbc4.Driver")
        DriverManager.getConnection(jdbcUrl, connectionProps)
    }

    val getSparkContext: (SparkContext, SQLContext) = {
        val sparkConf: SparkConf = new SparkConf().setAppName("Full Dump Testing").setMaster("local")
        val sc: SparkContext = new SparkContext(sparkConf)
        val sqlContext: SQLContext = new SQLContext(sc)

        System.setProperty("com.amazonaws.services.s3.enableV4", "true")
        sc.hadoopConfiguration.set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
        sc.hadoopConfiguration.set("fs.s3a.fast.upload", "true")
        (sc, sqlContext)
    }

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

    def dropTableRedshift(config: Config, tables: String*): Unit = {
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

    override protected def beforeAll(): Unit = {
        super.beforeAll()
        val (sc, sqlContext) = getSparkContext
        _sc = sc
        _sqlContext = sqlContext
    }

    override protected def afterAll(): Unit = {
        super.afterAll()
        _sc.stop()
    }
}
