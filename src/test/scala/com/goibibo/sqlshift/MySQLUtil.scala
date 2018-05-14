package com.goibibo.sqlshift

import java.net.URL
import java.sql.{Connection, DriverManager}
import java.util.Properties

import com.typesafe.config.Config
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source

/**
  * Project: sqlshift
  * Author: shivamsharma
  * Date: 5/8/18.
  */
object MySQLUtil {
    private val logger: Logger = LoggerFactory.getLogger(this.getClass)

    private def getMySQLConnection(config: Config): Connection = {
        val mysql = config.getConfig("mysql")
        val connectionProps = new Properties()
        connectionProps.put("user", mysql.getString("username"))
        connectionProps.put("password", mysql.getString("password"))
        val jdbcUrl = s"jdbc:mysql://${mysql.getString("hostname")}:${mysql.getInt("portno")}/${mysql.getString("db")}?createDatabaseIfNotExist=true&useSSL=false"
        Class.forName("com.mysql.jdbc.Driver")
        DriverManager.getConnection(jdbcUrl, connectionProps)
    }

    def createTableAndInsertRecords(config: Config, tableName: String, psvFile: URL): Unit = {
        logger.info("Inserting records in table: {}", tableName)
        val records = Source.fromFile(psvFile.toURI).getLines().toList.drop(1) // removing header

        val conn = getMySQLConnection(config)
        val statement = conn.createStatement()
        try {
            val tableCreateQuery = config.getString("table.tableCreateQuery").replace("${tableName}", tableName)
            logger.info("Running query: {}", tableCreateQuery)
            statement.executeUpdate(tableCreateQuery)
            val insertIntoQuery = config.getString("table.insertIntoQuery").replace("${tableName}", tableName)
            logger.info("Running query: {}", insertIntoQuery)
            records.foreach { record: String =>
                val columns = record.split("\\|")
                val query = insertIntoQuery.format(columns: _*)
                statement.executeUpdate(query)
            }
        } finally {
            statement.close()
            conn.close()
        }
    }
}
