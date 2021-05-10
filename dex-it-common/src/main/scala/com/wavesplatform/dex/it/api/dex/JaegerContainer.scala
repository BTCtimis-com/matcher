package com.wavesplatform.dex.it.api.dex

import com.dimafeng.testcontainers.FixedHostPortGenericContainer
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy
import org.testcontainers.utility.TestcontainersConfiguration

import java.net.InetAddress

object JaegerContainer {

  def createJaegerContainer(): Unit = {
    TestcontainersConfiguration.getInstance().updateUserConfig("testcontainers.reuse.enable", "true")
    new FixedHostPortGenericContainer(
      "jaegertracing/all-in-one:latest",
      waitStrategy = Some(new LogMessageWaitStrategy().withRegEx(".*Starting HTTP server.*")),
      exposedHostPort = 16686,
      exposedContainerPort = 16686
    ).configure { k =>
      k.withFixedExposedPort(14250, 14250)
      k.withFixedExposedPort(14268, 14268)
      k.withFixedExposedPort(6831, 6831)
      k.withReuse(true)
    }.start()
  }

  def getJaegerHttpUrl(): String = {
    val host = InetAddress.getLocalHost.getHostAddress
    s"http://$host:14268/api/traces"
  }

}
