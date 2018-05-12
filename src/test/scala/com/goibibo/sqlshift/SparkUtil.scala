package com.goibibo.sqlshift

import com.databricks.spark.redshift.RedshiftReaderM
import com.typesafe.config.Config
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Project: sqlshift
  * Author: shivamsharma
  * Date: 5/10/18.
  */
trait SparkUtil {
    def getSparkContext: (SparkContext, SQLContext) = {
        val sparkConf: SparkConf = new SparkConf().setAppName("Full Dump Testing").setMaster("local")
        val sc: SparkContext = new SparkContext(sparkConf)
        val sqlContext: SQLContext = new SQLContext(sc)

        System.setProperty("com.amazonaws.services.s3.enableV4", "true")
        sc.hadoopConfiguration.set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
        sc.hadoopConfiguration.set("fs.s3a.fast.upload", "true")
        (sc, sqlContext)
    }

    val (sc, sqlContext) = getSparkContext

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
}
