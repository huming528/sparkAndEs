package com.es.study

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext
import org.elasticsearch.spark.streaming._

import scala.collection.mutable

/**
  * DStream insert ES test example
  * Created by Administrator on 2019/3/27.
  */
object DStream {

  def dealDStream(sc:SparkContext): Unit ={

    //注意 测试代码 下面单个方法为一个独立体, 请分开运行,一次只放开一个方法

    //每秒写简单数据到es
    simpleDStreamInsert(sc)

    //每秒写case class数据到es
//    classDStreamInsert(sc)

    //写json数据
//    jsonDStreamInsert(sc)

    //动态写入
//    dynamicDStreamInsert(sc)

    //写入元数据
//    mateDStreamInsert(sc)
  }

  /**
    * 每秒写简单数据到es
    */
  def simpleDStreamInsert(sc : SparkContext): Unit ={

    val ssc = new StreamingContext(sc,Seconds(1))
    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

    //每秒写简单数据到es
    val rdd  = sc.makeRDD(Seq(numbers,airports))
    val microbatches = mutable.Queue(rdd)

    ssc.queueStream(microbatches).saveToEs("spark/docs")

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 每秒写case class数据到es
    */
  def classDStreamInsert(sc:SparkContext): Unit ={
    val ssc = new StreamingContext(sc,Seconds(1))
    //每秒写case class数据到es
    case class Trip(departure:String,arrival:String)
    var upcomingTrip = Trip("OTP","SFO")
    var lastWeekTrip = Trip("MUC","OTP")

    val rdd = sc.makeRDD(Seq(upcomingTrip,lastWeekTrip))
    val microbatches = mutable.Queue(rdd)
    val dstream = ssc.queueStream(microbatches)

    EsSparkStreaming.saveToEs(dstream,"spark/docs")
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 写json数据
    */
  def jsonDStreamInsert(sc : SparkContext): Unit ={
    val ssc = new StreamingContext(sc,Seconds(1))
    //写json数据
    val json1 = """{"reason" : "business", "airport" : "SFO"}"""
    val json2 = """{"participants" : 5, "airport" : "OTP"}"""

    val rdd = sc.makeRDD(Seq(json1,json2))
    val microbatch = mutable.Queue(rdd)
    val dStream = ssc.queueStream(microbatch)

    EsSparkStreaming.saveJsonToEs(dStream,"spark/docs")

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 动态写入
    * @return
    */
  def dynamicDStreamInsert(sc : SparkContext): Unit ={
    val ssc = new StreamingContext(sc,Seconds(1))
    val game = Map("media_type"->"game","title" -> "FF VI","year" -> "1994")
    val book = Map("media_type" -> "book","title" -> "Harry Potter","year" -> "2010")
    val cd = Map("media_type" -> "music","title" -> "Surfing With The Alien")

    var rdd = sc.makeRDD(Seq(game,book,cd))
    var microbatches = mutable.Queue(rdd)
    var dStream = ssc.queueStream(microbatches)

    EsSparkStreaming.saveToEs(dStream,"spark/doc_{media_type}")

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 写入元数据
    * @param sc
    */
  def mateDStreamInsert(sc : SparkContext): Unit ={
    val ssc = new StreamingContext(sc,Seconds(1))
    //写入元数据
    val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
    val muc = Map("iata" -> "MUC", "name" -> "Munich")
    val sfo = Map("iata" -> "SFO", "name" -> "San Fran")

    val airportsRdd = sc.makeRDD(Seq((1,otp),(2,muc),(3,sfo)))
    val microbathes = mutable.Queue(airportsRdd)

    ssc.queueStream(microbathes).saveToEsWithMeta("airports/2015")
    ssc.start()
  }
}
