package com.goibibo.sqlshift.services

import com.whisk.docker.{DockerContainer, DockerKit, DockerReadyChecker}

/**
  * Project: sqlshift
  * Author: shivamsharma
  * Date: 5/10/18.
  */
trait DockerSparkService extends DockerKit {
    lazy val sparkContainer: DockerContainer = DockerContainer("sequenceiq/spark:1.6.0")
            .withReadyChecker(DockerReadyChecker.LogLineContains("localhost: starting nodemanager, logging to /usr/local/hadoop/logs/yarn-root-nodemanager-9e2d264cafdd.out"))
            .withCommand("spark-submit", "--master", "local[*]", "--class", "com.goibibo.sqlshift.SQLShift", "--executor-cores", "3", "--executor-memory", "1g", "--num-executors", "2", "--driver-cores", "1 ", "--driver-memory", "1g", "./target/scala-2.10/sqlshift-assembly-0.1.jar", "-td", "src/test/resources/sqlshift.conf")

    abstract override def dockerContainers: List[DockerContainer] = sparkContainer :: super.dockerContainers
}
