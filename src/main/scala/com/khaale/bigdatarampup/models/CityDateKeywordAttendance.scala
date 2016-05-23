package com.khaale.bigdatarampup.models

/**
  * Created by Aleksander_Khanteev on 5/21/2016.
  */
case class CityDateKeywordAttendance(cityId:Int, date:Int, totalAttendance:Int, keywordAttendance: Array[(String,Int)])
