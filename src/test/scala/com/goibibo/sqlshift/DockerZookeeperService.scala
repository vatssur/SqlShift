package com.goibibo.sqlshift

import com.whisk.docker.{DockerContainer, DockerKit, DockerReadyChecker}

/**
  * Project: sqlshift
  * Author: shivamsharma
  * Date: 5/8/18.
  */
trait DockerZookeeperService extends DockerKit {
    val ZK_PORT: Int = 2181
    lazy val zkContainer: DockerContainer = DockerContainer("zookeeper:3.3.6")
            .withPorts(ZK_PORT -> Some(ZK_PORT))
            .withReadyChecker(DockerReadyChecker.LogLineContains("binding to port 0.0.0.0/0.0.0.0:2181"))

    abstract override def dockerContainers: List[DockerContainer] = zkContainer :: super.dockerContainers
}
