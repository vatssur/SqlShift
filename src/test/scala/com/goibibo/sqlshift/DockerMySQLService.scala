package com.goibibo.sqlshift

import com.whisk.docker.{DockerContainer, DockerKit, DockerReadyChecker}

/**
  * Project: sqlshift
  * Author: shivamsharma
  * Date: 5/8/18.
  */
trait DockerMySQLService extends DockerKit {
    val MYSQL_PORT: Int = 3306
    lazy val mySQLContainer: DockerContainer = DockerContainer("mysql:5.7.22")
            .withPorts(MYSQL_PORT -> Some(MYSQL_PORT))
            .withEnv("MYSQL_ROOT_PASSWORD=admin")
            .withReadyChecker(DockerReadyChecker.LogLineContains("mysqld: ready for connections."))

    abstract override def dockerContainers: List[DockerContainer] = mySQLContainer :: super.dockerContainers
}
