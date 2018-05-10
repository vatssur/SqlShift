package com.goibibo.sqlshift

import com.goibibo.sqlshift.commons.Util
import com.typesafe.config.{Config, ConfigFactory}
import com.whisk.docker.impl.spotify.DockerKitSpotify
import com.whisk.docker.scalatest.DockerTestKit
import org.apache.spark.sql.DataFrame
import org.scalatest.time.{Second, Seconds, Span}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success}

/**
  * Project: sqlshift
  * Author: shivamsharma
  * Date: 5/8/18.
  */
class FullDump extends FlatSpec
        with Matchers
        with GivenWhenThen
        with DockerTestKit
        with DockerKitSpotify
        with DockerMySQLService
        with DockerZookeeperService {

    private val logger: Logger = LoggerFactory.getLogger(this.getClass)
    implicit val pc: PatienceConfig = PatienceConfig(Span(20, Seconds), Span(1, Second))
    private val config: Config = ConfigFactory.load()
    private val (sc, sqlContext) = Util.getSparkContext

    def readTableFromRedshift(config: Config, tableName: String): DataFrame = {
        val redshift: Config = config.getConfig("redshift")
        println(s"jdbc:redshift://${redshift.getString("hostname")}:${redshift.getInt("portno")}/${redshift.getString("database")}")
        sqlContext
                .read
                .format("com.databricks.spark.redshift")
                .option("user", redshift.getString("username"))
                .option("password", redshift.getString("password"))
                .option("jdbcdriver", "com.amazon.redshift.jdbc4.Driver")
                .option("url", s"jdbc:redshift://${redshift.getString("hostname")}:${redshift.getInt("portno")}/${redshift.getString("database")}")
                .option("dbtable", tableName)
                .option("tempdir", config.getString("s3.location"))
                .load()
    }

    val data = new {


    }

    def setUpMySQL(): Unit = {
        val recordsFileName = config.getString("table.recordsFileName")
        logger.info(s"Inserting records in MySQL from file: $recordsFileName")
        isContainerReady(mySQLContainer) onComplete {
            case Success(posts) =>
                MySQLUtil.insertRecords(config, this.getClass.getClassLoader.getResourceAsStream(recordsFileName))
            case Failure(t) => logger.error("Error occurred making container ready", t)
        }
        logger.info("Insertion Done!!!")
    }


    "mongodb node" should "be ready with log line checker" in {
        val dataFrame = readTableFromRedshift(config, "sqlshift.test")
        dataFrame.show()
        setUpMySQL()
        Thread.sleep(10000)
    }
}
