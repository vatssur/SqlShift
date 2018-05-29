package com.goibibo.sqlshift.offsetmanagement

import java.util.Properties

import com.goibibo.sqlshift.models.Configurations.Offset

/**
  * Project: sqlshift
  * Author: shivamsharma
  * Date: 1/24/18.
  *
  * This manager is for maintaining offsets for each table specified in configuration.
  */
abstract class OffsetManager(conf: Properties, tableName: String) {

    /**
      * Returns the offset stored for specific table.
      *
      * @return Offset data for table
      */
    def getOffset: Option[Offset]

    /**
      * Set the offset value on specified table.
      *
      * @param value value to set
      * @return true of successful set or vice versa
      */
    def setOffset(value: Offset): Boolean

    /**
      * Get lock on table to prevent concurrent running application migrating same table.
      * This lock is specific to the table specified in configuration.
      * It returns true for acquiring lock and vice versa.
      *
      * @return
      */
    def getLock: Boolean

    /**
      * Release lock of specific table. Whether a lock is present or not this method
      * will always succeed.
      *
      * @return
      */
    def releaseLock(): Unit

    /**
      * Close connection when finished.
      */
    def close(): Unit
}
