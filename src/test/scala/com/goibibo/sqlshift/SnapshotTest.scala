package com.goibibo.sqlshift

import java.net.URL

import com.goibibo.sqlshift.commons.Util
import com.goibibo.sqlshift.models.Configurations
import com.goibibo.sqlshift.services.{DockerMySQLService, DockerZookeeperService}
import com.typesafe.config.{Config, ConfigFactory}
import com.whisk.docker.impl.spotify.DockerKitSpotify
import com.whisk.docker.scalatest.DockerTestKit
import org.apache.spark.sql.DataFrame
import org.scalatest.time.{Second, Seconds, Span}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Project: sqlshift
  * Author: surabhivatsa
  * Date: 6/1/18.
  */
class SnapshotTest extends FlatSpec
        with Matchers
        with GivenWhenThen
        with DockerTestKit
        with DockerKitSpotify
        with DockerMySQLService
        with DockerZookeeperService
        with SparkNRedshiftUtil {

    private val logger: Logger = LoggerFactory.getLogger(this.getClass)
    implicit val pc: PatienceConfig = PatienceConfig(Span(20, Seconds), Span(1, Second))

    private val fixtures = new {
        val config: Config = ConfigFactory.load("testsuite")
        val redshift: Config = config.getConfig("redshift")
        val recordsFileName: String = config.getString("table.recordsFileName")
        val recordsFile: URL = this.getClass.getClassLoader.getResource(recordsFileName)

        private val snapshotTables: Config = config.getConfig("tableNames.fullDump")
        val nonSplitTable: String = snapshotTables.getString("nonSplitTable")
        val splitTableWithoutDistKey: String = snapshotTables.getString("splitTableWithoutDistKey")
        val splitTableWithDistKey: String = snapshotTables.getString("splitTableWithDistKey")
        val listOfTables = List(nonSplitTable, splitTableWithoutDistKey, splitTableWithDistKey)
        var pAppConfiguration: Configurations.PAppConfiguration = _
    }

    private trait PSVData {
        val psvFullDumpRdd: DataFrame = sqlContext
                .read
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("delimiter", "|")
                .load(fixtures.recordsFile.getFile)
    }

    def setUpMySQL(): Unit = {
        import fixtures._
        logger.info(s"Inserting records in MySQL from file: $recordsFileName")
        val isContainerReadyBool = Await.result(isContainerReady(mySQLContainer), 20 seconds)
        if (isContainerReadyBool) {
            listOfTables.foreach { tableName =>
                MySQLUtil.createTableAndInsertRecords(config, tableName, recordsFile)
            }
        }
        else {
            logger.error("Error occurred making container ready")
        }
        logger.info("Insertion Done!!!")
    }

    def startSqlShift(): Configurations.PAppConfiguration = {
        val url = this.getClass.getClassLoader.getResource("snapshot.conf")
        fixtures.pAppConfiguration = Util.getAppConfigurations(url.getFile)
        SQLShift.start(sqlContext, fixtures.pAppConfiguration, 0)
    }

    override def beforeAll(): Unit = {
        super.beforeAll()
        setUpMySQL()
        val pAppConfiguration = startSqlShift()
        logger.info("\n{}", Util.formattedInfoSection(pAppConfiguration))
    }

    "Full Dump(Non Splittable) to redshift" should "have equal count" in new PSVData {
        import fixtures._

        val tableName: String = config.getString("redshift.schema") + "." + nonSplitTable
        logger.info(s"Fetching table $tableName from redshift.")
        val redshiftFullDumpDataFrame: DataFrame = readTableFromRedshift(config, tableName)

        redshiftFullDumpDataFrame.count should equal(psvFullDumpRdd.count)
    }

    "Full Dump(Non Splittable) to redshift" should "have same records" in new PSVData {
        import fixtures._

        val tableName: String = config.getString("redshift.schema") + "." + nonSplitTable
        logger.info(s"Fetching table $tableName from redshift.")
        val redshiftFullDumpDataFrame: DataFrame = readTableFromRedshift(config, tableName)

        redshiftFullDumpDataFrame.except(psvFullDumpRdd).count() should equal(0)
    }

    "Full Dump(Splittable With DistKey) to redshift" should "have equal count" in new PSVData {
        import fixtures._

        val tableName: String = config.getString("redshift.schema") + "." + splitTableWithDistKey
        logger.info(s"Fetching table $tableName from redshift.")
        val redshiftFullDumpDataFrame: DataFrame = readTableFromRedshift(config, tableName)

        redshiftFullDumpDataFrame.count should equal(psvFullDumpRdd.count)
    }

    "Full Dump(Splittable With DistKey) to redshift" should "have same records" in new PSVData {
        import fixtures._

        val tableName: String = config.getString("redshift.schema") + "." + splitTableWithDistKey
        logger.info(s"Fetching table $tableName from redshift.")
        val redshiftFullDumpDataFrame: DataFrame = readTableFromRedshift(config, tableName)

        redshiftFullDumpDataFrame.except(psvFullDumpRdd).count() should equal(0)
    }

    "Full Dump(Splittable Without DistKey) to redshift" should "fail if table not have primary key" in new PSVData {
        import fixtures._

        val tableName: String = config.getString("redshift.schema") + "." + splitTableWithoutDistKey
        logger.info(s"Fetching table $tableName from redshift.")
        val redshiftFullDumpDataFrame: DataFrame = readTableFromRedshift(config, tableName)

        redshiftFullDumpDataFrame.count should equal(psvFullDumpRdd.count)
    }

    override def afterAll(): Unit = {
        import fixtures._
        super.afterAll()
        val redshiftTablesToClean = listOfTables.map(table => config.getString("redshift.schema") + "." + table)
        dropTableRedshift(config, redshiftTablesToClean: _*)
    }
}
