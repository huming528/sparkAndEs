package com.es.study

import org.apache.spark.SparkContext
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.rdd.Metadata._
/**
  * Created by Administrator on 2019/3/26.
  */
object SimpleCURD {
  def elasticSearchCURD(sc : SparkContext): Unit ={
    var numbers = Map("id"->100,"one" -> 1,"two" ->2,"three" -> 3)
    var airports = Map("id"->101,"arrival" -> "Otopeni", "SFO" -> "San Fran")

    //简单的插入
    sc.makeRDD(Seq(numbers,airports)).saveToEs("hmh_index/simple_curd",Map("es.mapping.id"->"id"))

    //define a case class
    case class Trip(id:String,departure: String ,arrival: String)
    val upcomingTrip = Trip("1","OTP","SFO")
    val lastWeekTrip = Trip("2","MUC","OTP")

    val rdd = sc.makeRDD(Seq(upcomingTrip,lastWeekTrip))

    //通过对象实例插入
//    EsSpark.saveToEs(rdd,"hmh_index/docs",Map("es.mapping.id"-> "id"))

    val json1 = """{"id":100,"reason":"business","airport":"SFO"}"""
    val json2 = """{"id":101,"participants":5,"airport":"OTP"}"""

    //插入json数据
//    sc.makeRDD(Seq(json1,json2)).saveJsonToEs("spark/json-trips",Map("es.mapping.id"->"id"))

    val game = Map("media_type"->"game","title"->"FF VI","year"->"1992")
    val book = Map("media_type"->"book","title"->"Harry Potter","year"->"2010")
    val music = Map("media_type"->"music","title"->"Surfing With The Alien")

    //动态插入索引、类型
//    sc.makeRDD(Seq(game,book,music)).saveToEs("spark/doc_{media_type}")

    val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
    val muc = Map("iata" -> "MUC", "name" -> "Munich")
    val sfo = Map("iata" -> "SFO", "name" -> "San Fran")

    //处理文档元数据 插入ID
//    var airportsRDD = sc.makeRDD(Seq((1,otp),(2,muc),(3,sfo)))
//    airportsRDD.saveToEsWithMeta("spark/2015")

    val otpMeta = Map(ID -> 1, TTL -> "3h")
    val mucMeta = Map(ID -> 2,VERSION ->"23")
    val sfoMeta = Map(ID ->3)

    //处理文档元数据 插入ID TTL VERSION
//    val airportsRDD = sc.makeRDD(Seq((otpMeta,otp),(mucMeta,muc),(sfoMeta,sfo)))
//    airportsRDD.saveToEsWithMeta("spark/2015")

    //查询索引数据 循环遍历
    val sparkRDD = sc.esRDD("spark/2015")
    sparkRDD.foreachPartition(p => p.foreach(x=>print("all_value:"+x)))


    //带条件查询
    val queryRdd1 = sc.esRDD("spark/2015","?q=*O*")//查询所有字段
    val queryRdd2 = sc.esRDD("spark/2015","?q=iata:OT*") //查询某一个字段

    queryRdd1.foreachPartition(p=>p.foreach(x =>print("queryRdd1:"+x)))
    queryRdd2.foreachPartition(p=>p.foreach(x =>print("queryRdd2:"+x)))

  }

}
