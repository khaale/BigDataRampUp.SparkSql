package com.khaale.bigdatarampup.etl

import com.khaale.bigdatarampup.models.{CityDateKeywordAttendance, CityDateTagIds, DicCity, DicTags}
import com.khaale.bigdatarampup.shared.{FacebookSettings, WithNotifications}
import com.khaale.bigdatarampup.shared.facebook.FbFacade
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SQLContext}
import org.apache.spark.{Accumulator, Logging, SparkContext}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

/**
  * Created by Aleksander_Khanteev on 5/21/2016.
  */
class AttendanceProcessor extends Logging with Serializable {

  def process(sc:SparkContext,
              tagsBroadcast:Broadcast[Map[Long,DicTags]],
              citiesBroadcast:Broadcast[Map[Int,DicCity]],
              input:Dataset[CityDateTagIds],
              opts:AttendanceProcessorOpts = AttendanceProcessorOpts()
             //): Dataset[CityDateKeywordAttendance] = {
             ): Dataset[(Int,Int,Int,Array[(String,Int)])] = {

    val sqlc = SQLContext.getOrCreate(sc)
    import sqlc.implicits._

    val output = input
       .groupBy(x => x.cityId)
       .mapGroups((cityId, cityData) =>
         CityAttendanceMapper.mapCityAttendance(
           cityId,
           cityData.toArray,
           tagsBroadcast.value,
           citiesBroadcast.value,
           opts.fbSettings
         ).map(x => (x.cityId, x.date, x.totalAttendance, x.keywordAttendance)))
       .flatMap(x => x)

    output
  }
}

object CityAttendanceMapper {

  def mapCityAttendance(
                         cityId:Int,
                         cityRecords:Array[CityDateTagIds],
                         tagsMap:Map[Long,DicTags],
                         citiesMap:Map[Int,DicCity],
                         fbSettings:Option[FacebookSettings]
                       ): Array[CityDateKeywordAttendance] = {

    val city = citiesMap.get(cityId)

    val dateKeywordsMap = cityRecords
      .map(x => x.date -> x.mapTags(tagsMap))
      .toMap
    val uniqueKeywords = dateKeywordsMap.flatMap{ case(dt, keywords) => keywords}.toArray.distinct

    val keywordAttendanceMap =
      if (city.isDefined) getAttendance(uniqueKeywords, city.get, fbSettings) else {
        Map.empty[String,Int]
      }

    cityRecords.map(
      x => {
        val keywordAttendance = dateKeywordsMap(x.date)
          .map(kw => kw -> keywordAttendanceMap.getOrElse(kw, 0))
          .sortBy { case (kw, cnt) => -cnt }
        val totalAttendance = keywordAttendance.map(_._2).sum

        CityDateKeywordAttendance(x.cityId, x.date, totalAttendance, keywordAttendance)
      })
  }

  def getAttendance(
                     keywords:Array[String],
                     city:DicCity,
                     facebookSettings: Option[FacebookSettings]
                   ): Map[String,Int] = {

    facebookSettings match {
      case Some(settings) =>
        //working with real FB API
        val fbFacade = new FbFacade(
          facebookSettings.get.token
        )
        val keywordPlacesMap = fbFacade.getPlaces(keywords, city.latitude -> city.longitude, city.getSearchDistance)
        fbFacade.getEvents(keywordPlacesMap)
      case None =>
          //using fake FB data
          val rnd = scala.util.Random
          keywords.map(kw => kw -> rnd.nextInt(10000)).toMap
      }
  }
}

case class AttendanceProcessorOpts(fbSettings:Option[FacebookSettings] = None)
