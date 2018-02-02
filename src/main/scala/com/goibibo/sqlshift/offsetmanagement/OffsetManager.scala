package com.goibibo.sqlshift.offsetmanagement

/**
  * Project: sqlshift
  * Author: shivamsharma
  * Date: 1/24/18.
  *
  * This manager is for maintaining offsets for each table specified in configuration.
  */
trait OffsetManager {

    /**
      * Returns the offset stored for specific table.
      *
      * @return Offset data for table
      */
    def getOffset(tableName: String): Option[String]

    /**
      * Set the offset value on specified table.
      *
      * @param value value to set
      * @return true of successful set or vice versa
      */
    def setOffset(tableName: String, value: String): Boolean

    /**
      * Get lock on table to prevent concurrent running application migrating same table.
      * This lock is specific to specific table specified in configuration.
      * It returns true for acquiring lock and vice versa.
      *
      * @return
      */
    def getLock(tableName: String): Boolean

    /**
      * Release lock of specific table. Whether a lock is present or not this method
      * will always succeed.
      *
      * @return
      */
    def releaseLock(tableName: String): Unit
}
