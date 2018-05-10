package com.goibibo.sqlshift

import com.databricks.spark.redshift.RedshiftReaderM
import com.typesafe.config.{Config, ConfigFactory}
import com.whisk.docker.impl.spotify.DockerKitSpotify
import com.whisk.docker.scalatest.DockerTestKit
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
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

    def getSparkContext: (SparkContext, SQLContext) = {
        val sparkConf: SparkConf = new SparkConf().setAppName("Full Dump Testing").setMaster("local[1]")
        val sc: SparkContext = new SparkContext(sparkConf)
        val sqlContext: SQLContext = new SQLContext(sc)

        System.setProperty("com.amazonaws.services.s3.enableV4", "true")
        sc.hadoopConfiguration.set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
        (sc, sqlContext)
    }

    private val (sc, sqlContext) = getSparkContext

    def readTableFromRedshift(config: Config, tableName: String): DataFrame = {
        val redshift: Config = config.getConfig("redshift")
        val options = Map("dbtable" -> tableName,
            "url" -> s"jdbc:redshift://${redshift.getString("hostname")}:${redshift.getInt("portno")}/${redshift.getString("database")}",
            "tempdir" -> config.getString("s3.location"),
            "aws_iam_role" -> config.getString("redshift.iamRole")
        )
        RedshiftReaderM.getDataFrameForConfig(options, sc, sqlContext)
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
