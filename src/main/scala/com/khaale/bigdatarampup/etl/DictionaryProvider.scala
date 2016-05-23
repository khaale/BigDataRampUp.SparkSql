package com.khaale.bigdatarampup.etl

import com.khaale.bigdatarampup.models.{DicCity, DicTags}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SQLContext}

/**
  * Created by Aleksander_Khanteev on 5/21/2016.
  */
class DictionaryProvider(sc:SparkContext) {

  val sqlc = SQLContext.getOrCreate(sc)
  import sqlc.implicits._

  def loadTags(): Dataset[DicTags] = {
    sqlc.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("delimiter", "\t")
      .load("/data/advertising/dic/user.profile.tags.us.txt")
      .select($"ID".alias("id"), $"Keyword Value".alias("keywords"))
      .as[DicTags]
  }

  def loadCities(): Dataset[DicCity] = {
    sqlc.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("delimiter", "\t")
      .load("/data/advertising/dic/city.us.txt")
      .select($"Id".alias("id"), $"City".alias("name"), $"Latitude".alias("latitude"), $"Longitude".alias("longitude"), $"Area".alias("area"))
      .as[DicCity]
  }
}
