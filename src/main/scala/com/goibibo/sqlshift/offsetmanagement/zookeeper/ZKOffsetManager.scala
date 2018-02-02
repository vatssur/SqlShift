package com.goibibo.sqlshift.offsetmanagement.zookeeper

import com.goibibo.dp.utils.ZkUtils
import com.goibibo.sqlshift.offsetmanagement.{OffsetManager, ServiceConnectionException}
import org.apache.zookeeper.ZooKeeper
import org.slf4j.{Logger, LoggerFactory}

/**
  * Project: sqlshift
  * Author: shivamsharma
  * Date: 1/25/18.
  */
class ZKOffsetManager(zkquoram: String, path: String) extends OffsetManager {
    private val logger: Logger = LoggerFactory.getLogger(this.getClass)
    private val zkClient: Option[ZooKeeper] = ZkUtils.connect(zkquoram)
    private val dataNode: String = "000.data"
    private val lockNode: String = "000.lock"

    /**
      * Returns the offset stored for specific table.
      *
      * @return Offset data for table
      */
    override def getOffset(tableName: String): Option[String] = {
        if(zkClient.isEmpty) {
            logger.warn("Zookeeper is not connected. Skipping Table {}.....", tableName)
            throw new ServiceConnectionException(s"Zookeeper($zkquoram) is not connected")
        }
        implicit val zooKeeper: ZooKeeper = zkClient.get
        ZkUtils.getNodeDataAsString(path + "/" + tableName + "/" + dataNode)
    }

    /**
      * Set the offset value on specified table.
      *
      * @param value value to set
      * @return true of successful set or vice versa
      */
    override def setOffset(tableName: String, value: String): Boolean = {
        if(zkClient.isEmpty) {
            logger.warn("Zookeeper is not connected. Skipping Table {}.....", tableName)
            throw new ServiceConnectionException(s"Zookeeper($zkquoram) is not connected")
        }
        implicit val zooKeeper: ZooKeeper = zkClient.get
        ZkUtils.upsert(path + "/" + tableName + "/" + dataNode, value.getBytes)
    }

    /**
      * Get lock on table to prevent concurrent running application migrating same table.
      * This lock is specific to specific table specified in configuration.
      * It returns true for acquiring lock and vice versa.
      *
      * @return
      */
    override def getLock(tableName: String): Boolean = {
        if(zkClient.isEmpty) {
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
    override def releaseLock(tableName: String): Unit = {
        if(zkClient.isEmpty) {
            logger.warn("Zookeeper is not connected. Skipping Table {}.....", tableName)
            throw new ServiceConnectionException(s"Zookeeper($zkquoram) is not connected")
        }
        zkClient.get.close()
    }
}
