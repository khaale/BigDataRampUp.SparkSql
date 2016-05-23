package com.khaale.bigdatarampup.models

/**
  * Created by Aleksander_Khanteev on 5/21/2016.
  */
case class DicTags(id: Long, keywords:String) {
  def splitKeywords() : Array[String] = {
    keywords.split(",")
  }
}
