package com.khaale.bigdatarampup.models

/**
  * Created by Aleksander_Khanteev on 5/21/2016.
  */
case class DicCity(id: Int, name: String, latitude:Double, longitude:Double, area:Double) {
  def getSearchDistance : Int = {
    Math.sqrt(area).toInt * 1000
  }
}
