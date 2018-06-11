package com.goibibo.sqlshift

import java.net.URL
import java.util.Properties

import com.goibibo.sqlshift.commons.Util
import com.goibibo.sqlshift.models.Configurations
import com.goibibo.sqlshift.offsetmanagement.zookeeper.ZKOffsetManager
import com.goibibo.sqlshift.services.{DockerMySQLService, DockerZookeeperService}
import com.typesafe.config.{Config, ConfigFactory}
import com.whisk.docker.impl.spotify.DockerKitSpotify
import com.whisk.docker.scalatest.DockerTestKit
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.time.{Second, Seconds, Span}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Project: sqlshift
  * Author: shivamsharma
  * Date: 5/16/18.
  */
class IncrementalTest extends FlatSpec
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

        private val fullDumpTables: Config = config.getConfig("tableNames.incremental")
        val zkProp = new Properties()
        zkProp.setProperty("zkquoram", config.getString("zookeeper.zkquoram"))
        zkProp.setProperty("path", config.getString("zookeeper.path"))
        val nonSplitTable: String = fullDumpTables.getString("nonSplitTable")
        val splitTableWithoutDistKey: String = fullDumpTables.getString("splitTableWithoutDistKey")
        val splitTableWithDistKey: String = fullDumpTables.getString("splitTableWithDistKey")
        val incrementalAutoIncremental: String = fullDumpTables.getString("incrementalAutoIncremental")
        val listOfTables = List(nonSplitTable, splitTableWithoutDistKey, splitTableWithDistKey, incrementalAutoIncremental)
        var pAppConfiguration: Configurations.PAppConfiguration = _

        val columnName: String = config.getString("incrementalConditions.columnName")
        val firstToOffset: String = config.getString("incrementalConditions.firstToOffset")
        val secondToOffset: String = config.getString("incrementalConditions.secondToOffset")
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
        val url = this.getClass.getClassLoader.getResource("incremental.conf")
        fixtures.pAppConfiguration = Util.getAppConfigurations(url.getFile)
        SQLShift.start(sqlContext, fixtures.pAppConfiguration, 0)
    }

    override def beforeAll(): Unit = {
        super.beforeAll()
        setUpMySQL()
        val pAppConfiguration = startSqlShift()
        logger.info("\n{}", Util.formattedInfoSection(pAppConfiguration))
    }

    "Incremental Dump(Non Splittable) to redshift" should "have equal count for first toOffset" in new PSVData {

        import fixtures._

        val tableName: String = config.getString("redshift.schema") + "." + nonSplitTable
        logger.info(s"Fetching table $tableName from redshift.")
        val redshiftDataFrame: DataFrame = readTableFromRedshift(config, tableName)
        private val newDataFrame: DataFrame = psvFullDumpRdd.where(s"""$columnName <= "$firstToOffset"""")
        redshiftDataFrame.count should equal(newDataFrame.count)
    }

    "Incremental Dump(Non Splittable) to redshift" should "have same records for first toOffset" in new PSVData {

        import fixtures._

        val tableName: String = config.getString("redshift.schema") + "." + nonSplitTable
        logger.info(s"Fetching table $tableName from redshift.")
        val redshiftDataFrame: DataFrame = readTableFromRedshift(config, tableName)
        private val newDataFrame: DataFrame = psvFullDumpRdd.where(s"""$columnName <= "$firstToOffset"""")
        redshiftDataFrame.except(newDataFrame).count() should equal(0)
    }

    "Incremental Dump(Splittable With DistKey) to redshift" should "have equal count for first toOffset" in new PSVData {

        import fixtures._

        val tableName: String = config.getString("redshift.schema") + "." + splitTableWithDistKey
        logger.info(s"Fetching table $tableName from redshift.")
        val redshiftDataFrame: DataFrame = readTableFromRedshift(config, tableName)
        private val newDataFrame: DataFrame = psvFullDumpRdd.where(s"""$columnName <= "$secondToOffset"""")
        redshiftDataFrame.count should equal(newDataFrame.count)
    }

    "Incremental Dump(Splittable With DistKey) to redshift" should "have same records for second toOffset" in new PSVData {

        import fixtures._

        val tableName: String = config.getString("redshift.schema") + "." + splitTableWithDistKey
        logger.info(s"Fetching table $tableName from redshift.")
        val redshiftFullDumpDataFrame: DataFrame = readTableFromRedshift(config, tableName)
        private val newDataFrame: DataFrame = psvFullDumpRdd.where(s"""$columnName <= "$secondToOffset"""")
        redshiftFullDumpDataFrame.except(newDataFrame).count() should equal(0)
    }

    "Incremental Dump(Splittable Without DistKey) to redshift" should "have equal count for second toOffset" in new PSVData {

        import fixtures._

        val tableName: String = config.getString("redshift.schema") + "." + splitTableWithoutDistKey
        logger.info(s"Fetching table $tableName from redshift.")
        val redshiftFullDumpDataFrame: DataFrame = readTableFromRedshift(config, tableName)
        private val newDataFrame: DataFrame = psvFullDumpRdd.where(s"""$columnName <= "$firstToOffset"""")
        redshiftFullDumpDataFrame.count should equal(newDataFrame.count)
    }

    "Incremental Dump(Splittable Without DistKey) to redshift" should "have same records for first toOffset" in new PSVData {

        import fixtures._

        val tableName: String = config.getString("redshift.schema") + "." + splitTableWithoutDistKey
        logger.info(s"Fetching table $tableName from redshift.")
        val redshiftFullDumpDataFrame: DataFrame = readTableFromRedshift(config, tableName)
        private val newDataFrame: DataFrame = psvFullDumpRdd.where(s"""$columnName <= "$firstToOffset"""")
        redshiftFullDumpDataFrame.except(newDataFrame).count() should equal(0)
    }

    "AutoIncremental to redshift on column" should "have latest value of column in redshift & zookeeper" in new PSVData {

        import fixtures._

        val tableName: String = config.getString("redshift.schema") + "." + incrementalAutoIncremental
        logger.info(s"Fetching table $tableName from redshift.")
        private val offsetManager = new ZKOffsetManager(zkProp, tableName = tableName)
        val redshiftDataFrame: DataFrame = readTableFromRedshift(config, tableName)
        val Row(maxColumnValueInRedshiftDF: String) = redshiftDataFrame.agg(max(s"$columnName")).head
        val Row(maxColumnValueInPSVDF: String) = psvFullDumpRdd.agg(max(s"$columnName")).head
        private val dataInOffsetManager: String = offsetManager.getOffset.get.data.get.split("\\.").head
        maxColumnValueInRedshiftDF should equal(maxColumnValueInPSVDF)
        maxColumnValueInRedshiftDF should equal(dataInOffsetManager)
    }

    override def afterAll(): Unit = {
        import fixtures._
        super.afterAll()
        val redshiftTablesToClean = listOfTables.map(table => config.getString("redshift.schema") + "." + table)
        dropTableRedshift(config, redshiftTablesToClean: _*)
    }
}
