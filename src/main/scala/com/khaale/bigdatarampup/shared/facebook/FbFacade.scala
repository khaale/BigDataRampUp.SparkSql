package com.khaale.bigdatarampup.shared.facebook

import com.restfb.batch.BatchRequest.BatchRequestBuilder
import com.restfb.batch.{BatchRequest, BatchResponse}
import com.restfb.json.{JsonArray, JsonObject}
import com.restfb.{DefaultFacebookClient, DefaultJsonMapper, Version}
import org.apache.spark.Logging

import scala.collection.JavaConversions._

/**
  * Created by Aleksander_Khanteev on 5/19/2016.
  */
class FbFacade(
                token:String,
                onSuccess:String=>Unit = s => {},
                onError:String=>Unit = s => {}
              ) extends Logging {


  private val fbClient = new DefaultFacebookClient(token, Version.VERSION_2_6)
  private val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
  format.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))

  def getPlaces(keywords: Array[String], location:(Double,Double), distance:Int): Map[String,Array[Long]] = {

    val placesRequests = keywords.map(kw => BatchRequestWithPayload(new BatchRequestBuilder("search").parameters(
      com.restfb.Parameter.`with`("type", "place")
      ,com.restfb.Parameter.`with`("q", kw)
      ,com.restfb.Parameter.`with`("center", s"${location._1},${location._2}")
      ,com.restfb.Parameter.`with`("distance", distance)
      ,com.restfb.Parameter.`with`("limit", "50")
    ).build(), kw))

    val responses = executeWithRetry(placesRequests)

    val mapper = new DefaultJsonMapper()

    responses.map(x => x.payload -> mapper.toJavaList(x.batchResponse.getBody, classOf[com.restfb.types.Place]).map(_.getId.toLong).toArray).toMap
  }


  def getEvents(keywordPlaces:Map[String,Array[Long]]) : Map[String,Int] = {

    def jsonArrayToArray(jArray:JsonArray) = {
      Range.apply(0, jArray.length()).map(i => jArray.getJsonObject(i))
    }

    val keywordPlacesBatched = keywordPlaces.mapValues(x => x.grouped(50).toArray)

    val s = keywordPlacesBatched
     .toArray
     .flatMap { case (kw, placeIdBatches) => placeIdBatches.map(kw -> _)}

    val eventRequests = s.map{ case(kw, placeIdBatch) =>
        BatchRequestWithPayload(new BatchRequestBuilder("").parameters(
            com.restfb.Parameter.`with`("ids",placeIdBatch.map(x => x).mkString(","))
           ,com.restfb.Parameter.`with`("fields",s"events.fields(id,name,attending_count,start_time)")).build(),
          kw)
    }

    val responses = executeWithRetry(eventRequests)

    val mapper = new DefaultJsonMapper()

    val keywordAttendence = responses
      .map(x =>
        x.payload -> mapper.toJavaObject(x.batchResponse.getBody, classOf[JsonObject]) )
      .map { case (kw, jsObj) =>
        kw -> jsObj.keys()
          .map(key => jsObj.getJsonObject(key.toString))
          .filter(_.has("events"))
          .flatMap(x => jsonArrayToArray(x.getJsonObject("events").getJsonArray("data")).map(_.getInt("attending_count")))
          .sum }

    keywordAttendence
      .groupBy(_._1)
      .mapValues(_.map(_._2).sum)
  }

  private def executeWithRetry[A](batches:Iterable[BatchRequestWithPayload[A]]) : Array[BatchResponseWithPayload[A]] = {

    batches.grouped(50).flatMap(b => {

        try {
          val result = fbClient.executeBatch(b.map(b => b.batchRequest).toList)
          onSuccess("")
          result.zip(b).map(x => BatchResponseWithPayload(x._1, x._2.payload))
        }
        catch {
          case ex:com.restfb.exception.FacebookJsonMappingException =>
            logInfo("FB error: ", ex)
            onError("")
            Thread.sleep(1*60*1000)

            val result = fbClient.executeBatch(b.map(b => b.batchRequest).toList)
            onSuccess("")
            result.zip(b).map(x => BatchResponseWithPayload(x._1, x._2.payload))
        }
    }).toArray
  }

  case class BatchRequestWithPayload[A](batchRequest: BatchRequest, payload:A)
  case class BatchResponseWithPayload[A](batchResponse: BatchResponse, payload:A)
}
