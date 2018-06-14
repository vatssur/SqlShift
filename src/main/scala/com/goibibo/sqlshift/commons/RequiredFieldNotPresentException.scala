package com.goibibo.sqlshift.commons

/**
  * Project: sqlshift
  * Date: 1/30/18.
  *
  * This is exception raised when a required field in configuration is not present
  */
class RequiredFieldNotPresentException(msg: String) extends Exception(msg)