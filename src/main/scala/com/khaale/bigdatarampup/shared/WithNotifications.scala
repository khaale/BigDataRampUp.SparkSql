package com.khaale.bigdatarampup.shared

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
  * Created by Aleksander_Khanteev on 5/21/2016.
  */
object WithNotifications {

  def execute[A](timeoutMs:Int = 10*1000)(notifyFunc: ()=> Unit)(mainFunc: ()=>A): A= {

    import scala.concurrent.ExecutionContext.Implicits.global
    @volatile var terminated = false
    val counterPrintFuture = Future {
      while (!terminated) {
        Thread.sleep(timeoutMs)
        notifyFunc()
      }
      notifyFunc()
    }

    val result = mainFunc()

    terminated = true
    //noinspection LanguageFeature
    Await.ready(counterPrintFuture, 15 seconds)

    result
  }
}
