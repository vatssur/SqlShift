package com.goibibo.sqlshift.offsetmanagement.zookeeper

import java.util.Properties

import com.goibibo.dp.utils.ZkUtils
import com.goibibo.sqlshift.models.Configurations.Offset
import com.goibibo.sqlshift.offsetmanagement.{OffsetManager, ServiceConnectionException}
import org.apache.zookeeper.ZooKeeper
import org.json4s.DefaultFormats
import org.slf4j.{Logger, LoggerFactory}
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

/**
  * Project: sqlshift
  * Author: shivamsharma
  * Date: 1/25/18.
  */
class ZKOffsetManager(conf: Properties, tableName: String) extends OffsetManager(conf, tableName) {
    private val logger: Logger = LoggerFactory.getLogger(this.getClass)
    private val PATH_SEPARATOR = "/"
    private val dataNode: String = "000.data"
    private val lockNode: String = "000.lock"
    private val zkquoram = conf.getProperty("zkquoram")
    private val path = conf.getProperty("path")
    implicit val formats: DefaultFormats.type = DefaultFormats

    private val zkClient: Option[ZooKeeper] = {
        val maybeKeeper = ZkUtils.connect(zkquoram)

        implicit val tempClient: ZooKeeper = maybeKeeper.get
        val dirs = path.split(PATH_SEPARATOR).filter(_.nonEmpty).toList :+ tableName
        var tempDir = ""
        dirs.foreach { dir =>
            tempDir = tempDir + "/" + dir
            if (!ZkUtils.nodeExists(tempDir).getOrElse(false))
                ZkUtils.createPersistentNode(tempDir, "")
        }
        maybeKeeper
    }

    /**
      * Returns the offset stored for specific table.
      *
      * @return Offset data for table
      */
    override def getOffset: Option[Offset] = {
        if (zkClient.isEmpty) {
            logger.warn("Zookeeper is not connected. Skipping Table {}.....", tableName)
            throw new ServiceConnectionException(s"Zookeeper($zkquoram) is not connected")
        }
        implicit val zooKeeper: ZooKeeper = zkClient.get
        val offset = ZkUtils.getNodeDataAsString(path + "/" + tableName + "/" + dataNode)
        if (offset.isDefined) {
            Some(parse(offset.get).extract[Offset])
        }
        else None
    }

    /**
      * Set the offset value on specified table.
      *
      * @param value value to set
      * @return true of successful set or vice versa
      */
    override def setOffset(value: Offset): Boolean = {
        if (zkClient.isEmpty) {
            logger.warn("Zookeeper is not connected. Skipping Table {}.....", tableName)
            throw new ServiceConnectionException(s"Zookeeper($zkquoram) is not connected")
        }
        implicit val zooKeeper: ZooKeeper = zkClient.get
        if (!ZkUtils.nodeExists(path + "/" + tableName).getOrElse(false))
            ZkUtils.createPersistentNode(path + "/" + tableName, "")
        ZkUtils.upsert(path + "/" + tableName + "/" + dataNode, Serialization.write(value).getBytes)
    }

    /**
      * Get lock on table to prevent concurrent running application migrating same table.
      * This lock is specific to specific table specified in configuration.
      * It returns true for acquiring lock and vice versa.
      *
      * @return
      */
    override def getLock: Boolean = {
        if (zkClient.isEmpty) {
            logger.warn("Zookeeper is not connected. Skipping Table {}.....", tableName)
            throw new ServiceConnectionException(s"Zookeeper($zkquoram) is not connected")
        }
        implicit val zooKeeper: ZooKeeper = zkClient.get
        ZkUtils.createEphemeralNode(path + "/" + tableName + "/" + lockNode, "")
    }

    /**
      * Release lock of specific table. Whether a lock is present or not this method
      * will always succeed.
      *
      * @return
      */
    override def releaseLock(): Unit = {
        if (zkClient.isEmpty) {
            logger.warn("Zookeeper is not connected. Skipping Table {}.....", tableName)
            throw new ServiceConnectionException(s"Zookeeper($zkquoram) is not connected")
        }
        zkClient.get.close()
    }

    /**
      * Close connection when finished.
      */
    override def close(): Unit = {
        zkClient.foreach(_.close())
    }
}
