package com.wavesplatform.dex.it.api.dex

import cats.data.NonEmptyList
import com.dimafeng.testcontainers.FixedHostPortGenericContainer
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy
import org.testcontainers.utility.TestcontainersConfiguration

import java.net.NetworkInterface
import cats.syntax.either._

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

  def getJaegerHttpUrl(): String =
    NonEmptyList.of("eth0", "enp0s31f6", "en0")
      .map(getHostIp)
      .reduceLeft(_ orElse _)
      .map(x => s"http://$x:14268/api/traces")
      .getOrElse(throw new RuntimeException("can't create jaeger host"))

  private def getHostIp(netInterface: String): Option[String] = Either.catchNonFatal {
    val inetAddresses = NetworkInterface.getByName(netInterface).getInetAddresses
    while (inetAddresses.hasMoreElements) {
      val ia = inetAddresses.nextElement
      if (!ia.isLinkLocalAddress)
        return Some(ia.getHostAddress)
    }
    None
  }.toOption.flatten

}
