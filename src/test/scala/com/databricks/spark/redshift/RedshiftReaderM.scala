package com.databricks.spark.redshift

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import org.apache.spark.SparkContext
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.{DataFrame, SQLContext}

object RedshiftReaderM {

    val endpoint = "s3.ap-south-1.amazonaws.com"

    def getS3Client(provider: AWSCredentials): AmazonS3Client = {
        val client = new AmazonS3Client(provider)
        client.setEndpoint(endpoint)
        client
    }

    def getDataFrameForConfig(configs: Map[String, String], sparkContext: SparkContext, sqlContext: SQLContext): DataFrame = {
        val source: DefaultSource = new DefaultSource(new JDBCWrapper(), getS3Client)
        val br: BaseRelation = source.createRelation(sqlContext, configs)
        sqlContext.baseRelationToDataFrame(br)
    }
}