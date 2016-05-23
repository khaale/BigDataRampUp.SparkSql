package com.khaale.bigdatarampup.models

/**
  * Created by Aleksander_Khanteev on 5/21/2016.
  */
case class CityDateTagIds(cityId:Int, date:Int, tagIds:Array[Long]) {

  private object StaticHolder {
    val pattern = "[0-9]".r
  }

  def mapTags(tagsMap:Map[Long,DicTags]): Array[String] = {
    tagIds
      .flatMap(id => tagsMap.getOrElse(id, DicTags(id, "")).splitKeywords())
      .filter(kw => kw == "*" || (kw.length > 2 && StaticHolder.pattern.findFirstIn(kw).isEmpty))
      .distinct
      .sorted
  }
}

