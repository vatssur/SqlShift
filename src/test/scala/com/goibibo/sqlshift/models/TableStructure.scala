package com.goibibo.sqlshift.models

import java.sql.Date

/**
  * Project: sqlshift
  * Author: shivamsharma
  * Date: 5/8/18.
  */
case class TableStructure(id: Int,
                          created: Date,
                          name: String)