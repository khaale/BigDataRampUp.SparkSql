import java.util.{Date, TimeZone}

import com.restfb.DefaultJsonMapper
import com.restfb.batch.BatchRequest.BatchRequestBuilder

import scala.collection.JavaConversions._
import com.restfb.json.{JsonArray, JsonObject}
import com.restfb.types.Place

val accessToken = "308045085989469|XPQATymskgui8TbDQt1t2UJhytI"
val facebookClient = new com.restfb.DefaultFacebookClient(accessToken, com.restfb.Version.LATEST)

val keywords = Array("*")
val lon = 40.6643
val lat = -73.9385
val distance=  27000

def getFbPlacesForKeywords(keywords: Array[String], lat:Double, lon: Double, distance:Int ) = {

  val placesRequests = keywords.map(kw => new BatchRequestBuilder("search").parameters(
    com.restfb.Parameter.`with`("type", "place")
    , com.restfb.Parameter.`with`("q", kw)
    , com.restfb.Parameter.`with`("center", s"$lat,$lon")
    , com.restfb.Parameter.`with`("distance", distance)
    , com.restfb.Parameter.`with`("limit", "500")
  ).build())

  val responses = facebookClient.executeBatch(placesRequests.toList)

  val mapper = new DefaultJsonMapper()

  responses.zip(keywords).map(x => x._2 -> mapper.toJavaList(x._1.getBody, classOf[Place]).map(_.getId))
}


def getFbPlacesForKeywordsWithRetry(keywords: Array[String], lat:Double, lon: Double, distance:Int) = {

  try {
    getFbPlacesForKeywords(keywords, lat, lon, distance)
  }
  catch {
    case ex:com.restfb.exception.FacebookJsonMappingException =>
      Thread.sleep(5*60*1000)
      getFbPlacesForKeywords(keywords, lat, lon, distance)
  }
}

val placeIds = getFbPlacesForKeywords(Array("*","car","accessories","music","home","mobile","iphone","devices"), 40.6643, -73.9385, 27000)



import java.util.Date
val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
format.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))

def getFbEventAttendance(placeIds:java.util.List[String], tsStart:Int, tsEnd:Int) = {
  val facebookClient = new com.restfb.DefaultFacebookClient("308045085989469|XPQATymskgui8TbDQt1t2UJhytI", com.restfb.Version.LATEST);
  val startDate = new Date(tsStart * 1000L).toInstant.toString
  val endDate = new Date(tsEnd * 1000L).toInstant.toString

  def jsonArrayToArray(jArray:JsonArray) = {
    Range.apply(0, jArray.length()).map(i => jArray.getJsonObject(i))
  }

  def dateTimeStringToTs(dts:String) = {
    format.parse(dts.substring(0,10)).getTime / 1000
  }

  val eventRequests = placeIds.grouped(50).map(ids =>  new BatchRequestBuilder("").parameters(
    com.restfb.Parameter.`with`("ids",ids.mkString(","))
    ,com.restfb.Parameter.`with`("fields",s"events.fields(id,name,attending_count,start_time).since($startDate).until($endDate)")
  ).build())

  val responses = facebookClient.executeBatch(eventRequests.toList)

  val mapper = new DefaultJsonMapper()

  val events = responses.flatMap(x => mapper.toJavaList(x.getBody, classOf[JsonObject]))

  events.flatMap(e => e.keys()
    .map(key => e.getJsonObject(key.toString))
    .filter(_.has("events"))
    .map(_.getJsonObject("events").getJsonArray("data")).flatMap(jsonArrayToArray(_)).map(x => dateTimeStringToTs(x.getString("start_time")) -> x.getInt("attending_count"))
  )
    .toList.groupBy(_._1).map(x => x._1 -> x._2.map(_._2).sum)
}
val events = getFbEventAttendance(placeIds(0)._2, 1370476800, 1370476800 + 86400*4 + 86400 - 1)