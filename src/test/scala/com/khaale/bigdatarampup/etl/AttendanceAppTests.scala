package com.khaale.bigdatarampup.etl

import com.khaale.bigdatarampup.apps.AttendanceApp
import com.khaale.bigdatarampup.models.{CityDateTagIds, DicCity, DicTags}
import org.scalatest.FunSuite

/**
  * Created by Aleksander_Khanteev on 5/20/2016.
  */
class CityAttendanceMapperTests extends FunSuite {


  test("should map city") {

    val input = Array(
      CityDateTagIds(1, 1370476800, Array(1)),
      CityDateTagIds(1, 1370476800 + 2*86400, Array(1)),
      CityDateTagIds(1, 1370476800 + 3*86400, Array(1,2)),
      CityDateTagIds(1, 1370476800 + 4*86400, Array(2))
    )
    val citiesMap = Map(1->DicCity(1, "New York", -73.9385, 40.6643, 783.8))
    val tagsMap = Map(1L->DicTags(1L, "cars,accessories"),2L->DicTags(2L, "mobile,iphone"))

    val result = CityAttendanceMapper.mapCityAttendance(1, input, tagsMap, citiesMap, None)

    assert(result.length > 0)
  }
}
