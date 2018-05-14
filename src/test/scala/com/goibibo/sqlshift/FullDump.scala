package com.goibibo.sqlshift

import com.goibibo.sqlshift.SQLShift
import com.goibibo.sqlshift.commons.Util
import com.goibibo.sqlshift.models.Configurations
import com.goibibo.sqlshift.services.{DockerMySQLService, DockerSparkService, DockerZookeeperService}
import com.typesafe.config.{Config, ConfigFactory}
import com.whisk.docker.impl.spotify.DockerKitSpotify
import com.whisk.docker.scalatest.DockerTestKit
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
        with DockerZookeeperService
        with SparkUtil {

    private val logger: Logger = LoggerFactory.getLogger(this.getClass)
    implicit val pc: PatienceConfig = PatienceConfig(Span(20, Seconds), Span(1, Second))
    private val config: Config = ConfigFactory.load()

    def setUpMySQL(): Unit = {
        val recordsFileName = config.getString("table.recordsFileName")
        logger.info(s"Inserting records in MySQL from file: $recordsFileName")
        isContainerReady(mySQLContainer) onComplete {
            case Success(posts) =>
                MySQLUtil.createTableAndInsertRecords(config, this.getClass.getClassLoader.getResourceAsStream(recordsFileName))
            case Failure(t) => logger.error("Error occurred making container ready", t)
        }
        logger.info("Insertion Done!!!")
    }

    def startSqlShift(): Configurations.PAppConfiguration  = {
        val url = this.getClass.getClassLoader.getResource("sqlshift.conf")
        val pAppConfigurations = Util.getAppConfigurations(url.getFile)
        SQLShift.start(sqlContext, pAppConfigurations, 0)
    }

    override def beforeAll(): Unit = {
        super.beforeAll()
        setUpMySQL()
        startSqlShift()
    }

    "Sqlshift Full Dump" should " take all data to redshift" in {
        Thread.sleep(10000)
    }
}
