package com.goibibo.sqlshift

import java.net.URL

import com.databricks.spark.redshift.RedshiftReaderM
import com.goibibo.sqlshift.commons.Util
import com.goibibo.sqlshift.models.Configurations
import com.goibibo.sqlshift.services.{DockerMySQLService, DockerZookeeperService}
import com.typesafe.config.{Config, ConfigFactory}
import com.whisk.docker.impl.spotify.DockerKitSpotify
import com.whisk.docker.scalatest.DockerTestKit
import org.scalatest.time.{Second, Seconds, Span}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

/**
  * Project: sqlshift
  * Author: shivamsharma
  * Date: 5/8/18.
  */
class SQLShiftTest extends FlatSpec
        with Matchers
        with GivenWhenThen
        with DockerTestKit
        with DockerKitSpotify
        with DockerMySQLService
        with DockerZookeeperService
        with SparkUtil {

    private val logger: Logger = LoggerFactory.getLogger(this.getClass)
    implicit val pc: PatienceConfig = PatienceConfig(Span(20, Seconds), Span(1, Second))

    private val fixtures = new {
        val config: Config = ConfigFactory.load()
        val redshift: Config = config.getConfig("redshift")
        val recordsFileName: String = config.getString("table.recordsFileName")
        val recordsFile: URL = this.getClass.getClassLoader.getResource(recordsFileName)
        val fullDumpTableName = "full_dump"
        val incrementalTableName = "incremental"
    }

    private trait PSVData {
        val psvFullDumpRdd: DataFrame = sqlContext
                .read
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("delimeter", "|")
                .load(fixtures.recordsFile.getFile)
    }

    def setUpMySQL(): Unit = {
        import fixtures._
        logger.info(s"Inserting records in MySQL from file: $recordsFileName")
        isContainerReady(mySQLContainer) onComplete {
            case Success(posts) =>
                List(fullDumpTableName, incrementalTableName).foreach { tableName =>
                    MySQLUtil.createTableAndInsertRecords(config, tableName, recordsFile)
                }
            case Failure(t) => logger.error("Error occurred making container ready", t)
        }
        logger.info("Insertion Done!!!")
    }

    def startSqlShift(): Configurations.PAppConfiguration = {
        val url = this.getClass.getClassLoader.getResource("sqlshift.conf")
        val pAppConfigurations = Util.getAppConfigurations(url.getFile)
        SQLShift.start(sqlContext, pAppConfigurations, 0)
    }

    override def beforeAll(): Unit = {
        super.beforeAll()
        setUpMySQL()
        startSqlShift()
    }

    "SQLShift Full Dump to redshift" should "have equal count and have same records" in new PSVData {
        import fixtures._
        val options = Map("dbtable" -> fullDumpTableName,
            "user" -> redshift.getString("username"),
            "password" -> redshift.getString("password"),
            "url" -> s"jdbc:redshift://${redshift.getString("hostname")}:${redshift.getInt("portno")}/${redshift.getString("database")}",
            "tempdir" -> config.getString("s3.location"),
            "aws_iam_role" -> config.getString("redshift.iamRole")
        )
        val redshiftFullDumpDataFrame: DataFrame = RedshiftReaderM.getDataFrameForConfig(options, sc, sqlContext)

        redshiftFullDumpDataFrame.count should equal(psvFullDumpRdd.count)
        redshiftFullDumpDataFrame.except(psvFullDumpRdd).count() should equal(0)
    }

    override def afterAll(): Unit = {
        super.afterAll()
    }
}
