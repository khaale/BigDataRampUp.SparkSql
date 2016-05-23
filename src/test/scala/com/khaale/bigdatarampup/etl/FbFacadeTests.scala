package com.khaale.bigdatarampup.etl

import com.khaale.bigdatarampup.shared.AppSettingsProvider
import com.khaale.bigdatarampup.shared.facebook.FbFacade
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Created by Aleksander_Khanteev on 5/19/2016.
  */
class FbFacadeTests extends FunSuite with BeforeAndAfterAll  {

  var fbToken: String = null

  override def beforeAll(): Unit = {

    val settingsProvider = new AppSettingsProvider("cfg/app.test.conf")

    settingsProvider.getFacebookSettings match {
      case Some(fbSettings) => fbToken = fbSettings.token
      case None => throw new IllegalArgumentException("Facebook settings must be provided! See app.conf.sample as an example")
    }
  }


  test("should get places") {

    val sut = new FbFacade(fbToken)

    val result = sut.getPlaces(Array("car","accessories","music","home","mobile","iphone","devices"), (40.6643, -73.9385), 27000)

    assert(result.nonEmpty)
  }

  test("should get events") {

    val sut = new FbFacade(fbToken)

    val result = sut.getEvents(Map(("car", Array(166440000040224L))))

    assert(result.nonEmpty)
  }
}
