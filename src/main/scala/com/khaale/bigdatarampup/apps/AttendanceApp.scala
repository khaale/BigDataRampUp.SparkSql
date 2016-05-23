package com.khaale.bigdatarampup.apps

import com.khaale.bigdatarampup.etl.{AttendanceProcessor, AttendanceProcessorOpts, DictionaryProvider}
import com.khaale.bigdatarampup.models.{CityDateTagIds, DicCity, DicTags}
import com.khaale.bigdatarampup.shared.{AppSettingsProvider, ConditionalApplicative}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SQLContext}
import org.apache.spark.{Logging, SparkConf, SparkContext}


/**
  * Created by Aleksander_Khanteev on 5/18/2016.
  */
object AttendanceApp extends App with Logging  {

  val sc = new SparkContext(new SparkConf())
  val sqlc =  SQLContext.getOrCreate(sc) //new HiveContext(sc)

  import ConditionalApplicative._
  import sqlc.implicits._

  run()

  def run() {

    //set up configuration
    val settingsProvider = args(0) match {
      case confPath if confPath != null && !confPath.isEmpty => new AppSettingsProvider(confPath)
      case _ => throw new IllegalArgumentException("Configuration path must be specified as a first argument!")
    }
    val isTest = settingsProvider.isTestRun

    //loading input
    val input = sqlc.read.parquet(settingsProvider.getInputPath)
      .select($"cityid".alias("cityId"), $"date".alias("date"), $"tagids".alias("tagIds"))
      .as[CityDateTagIds]
      .$if(isTest) { _.filter(x => x.cityId == 1) }
      .cache()

    //preparing broadcasts with dictionaries
    val dictionaryProvider = new DictionaryProvider(sc, settingsProvider.dictionarySettings)
    val citiesBroadcast = broadcastAsMap[Int, DicCity](dictionaryProvider.loadCities())(x => x.id)
    val tagsBroadcast = broadcastAsMap[Long, DicTags](dictionaryProvider.loadTags())(x => x.id)

    //processing
    val processor = new AttendanceProcessor()
    val output = processor.process(sc, tagsBroadcast, citiesBroadcast, input, AttendanceProcessorOpts(settingsProvider.getFacebookSettings))

    //working with results
    if (isTest) {
      output.show(100, truncate = false)
    }
    else {
      output.toDF().write.parquet(settingsProvider.getOutputPath)
    }

    //finishing
    sc.stop()
  }

  def broadcastAsMap[B,A](data:Dataset[A])(getId:A=>B): Broadcast[Map[B,A]] = {

    val map = data.collect().map(x => getId(x) -> x).toMap
    val broadcast = sc.broadcast(map)
    broadcast
  }
}





