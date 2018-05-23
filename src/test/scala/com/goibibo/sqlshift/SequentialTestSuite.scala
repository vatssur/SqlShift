package com.goibibo.sqlshift

import org.scalatest.Sequential

/**
  * Project: sqlshift
  * Author: shivamsharma
  * Date: 5/24/18.
  */
class SequentialTestSuite extends Sequential(new FullDumpTest, new IncrementalTest)
