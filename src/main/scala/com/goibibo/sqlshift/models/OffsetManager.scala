package com.goibibo.sqlshift.models

/**
  * Project: sqlshift
  * Author: shivamsharma
  * Date: 1/24/18.
  *
  * This manager is for maintaining offsets for each table specified in configuration.
  */
trait OffsetManager {

    /**
      * Returns the offset stored in a path.
      *
      * @param path path of offset file.(URL or Zookeeper)
      * @return
      */
    def getOffset(path: String): String

    /**
      * Set the offset value on specified path.
      *
      * @param path path of offset file.(URL or Zookeeper)
      * @param value value to set
      */
    def setOffset(path: String, value: String): Unit

    /**
      * Get lock on path to prevent concurrent running application migrating same table.
      * This lock is specific to specific table specified in configuration.
      * It returns true for acquiring lock and vice versa.
      *
      * @param path path of lock file.
      * @return
      */
    def getLock(path: String): Boolean

    /**
      * Release lock of specific table. Whether a lock is present or not this method
      * will always succeed.
      *
      * @param path path of lock file.
      * @return
      */
    def releaseLock(path: String): Unit
}
