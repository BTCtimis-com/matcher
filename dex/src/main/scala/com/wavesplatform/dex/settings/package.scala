package com.wavesplatform.dex

import com.typesafe.config.{Config, ConfigFactory}
import com.wavesplatform.dex.settings.utils.ConfigOps.ConfigOps
import pureconfig.ConfigSource

import scala.util.Try

package object settings {

  implicit def toConfigOps(config: Config): ConfigOps = new ConfigOps(config)

  private val format = new java.text.DecimalFormat("#.################")

  /** Formats amount or price */
  def formatValue(value: BigDecimal): String = format.format(value.bigDecimal)

  def loadMatcherSettings(userConfig: Config): Try[MatcherSettings] = Try {
    ConfigSource.fromConfig(loadConfig(userConfig)).at("waves.dex").loadOrThrow[MatcherSettings]
  }

  def loadConfig(userConfig: Config): Config = loadConfig(Some(userConfig))

  def loadConfig(maybeUserConfig: Option[Config]): Config = {

    val defaults = ConfigFactory.defaultOverrides()
    val external = maybeUserConfig.fold(defaults)(defaults.withFallback)

    external
      .withFallback(ConfigFactory.defaultApplication())
      .withFallback(ConfigFactory.defaultReference())
      .resolve()
  }

}
