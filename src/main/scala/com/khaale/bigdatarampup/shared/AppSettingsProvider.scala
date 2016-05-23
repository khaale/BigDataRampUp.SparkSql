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

  def dictionarySettings: DictionarySettings = {
    if (!appCfg.hasPath("dict")) {
      return DictionarySettings("/data/advertising/dic")
    }

    val dictCfg = appCfg.getConfig("dict")
    DictionarySettings(dictCfg.getString("directory"))
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

  def getInputPath: String = {
    if (appCfg.hasPath("input-path")) appCfg.getString("input-path") else "/data/advertising/city_date_tagIds"
  }

  def getOutputPath: String = {
    if (appCfg.hasPath("output-path")) appCfg.getString("output-path") else "/data/advertising/city_date_event_attendance"
  }
}

case class FacebookSettings(token:String)

case class DictionarySettings(citiesPath:String, tagsPath:String) {
  def this(dictionaryDirPath: String) = this(dictionaryDirPath + "/city.us.txt", dictionaryDirPath + "/user.profile.tags.us.txt")
}
object DictionarySettings {
  def apply(dictionaryDirPath: String) = new DictionarySettings(dictionaryDirPath)
}