package com.khaale.bigdatarampup.shared

import java.io.{File, FileNotFoundException}

import com.typesafe.config.ConfigFactory

/**
  * Created by Aleksander_Khanteev on 5/23/2016.
  */
class AppSettingsProvider(configPath:String) {

  private val cfgFile = new File(configPath)

  private val appCfg = cfgFile match  {
    case x if x.exists() => ConfigFactory.parseFile(cfgFile).getConfig("app")
    case _ => throw new IllegalArgumentException(s"Configuration file '$configPath' does not exist")
  }

  def getFacebookSettings: Option[FacebookSettings] = {

    if (!appCfg.hasPath("facebook")) {
      return None
    }

    val fbCfg = appCfg.getConfig("facebook")
    Some(FacebookSettings(fbCfg.getString("token")))
  }

  def isTestRun: Boolean = {
    appCfg.hasPath("is-test-run") && appCfg.getBoolean("is-test-run")
  }
}

case class FacebookSettings(token:String)
