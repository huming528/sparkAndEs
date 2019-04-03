package com.es.study

import org.apache.spark.SparkContext
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.rdd.Metadata._

/**
  *  insert and query ES index data simple test example
  * Created by Administrator on 2019/3/26.
  */
object SimpleCURD {
  def elasticSearchCURD(sc : SparkContext): Unit ={

    //简单的插入
    simpleInsert(sc)

    //通过对象实例插入
    objectInsert(sc)

    //插入json数据
    jsonInsert(sc)

    //动态插入索引、类型
    dynamicInsert(sc)

    //处理文档元数据 插入ID
    metadateInsert(sc)

    //处理文档元数据 插入ID TTL VERSION
    otherMeteInsert(sc)

    //查询索引数据 循环遍历
    selectAllDate(sc)

    //带条件查询
    queryIndexData(sc)

    //DSL语句查询
    dslQueryIndexData(sc)


  }

  /**
    * 简单的插入
    * @param sc
    */
  def simpleInsert(sc : SparkContext): Unit ={
    var numbers = Map("id"->100,"one" -> 1,"two" ->2,"three" -> 3)
    var airports = Map("id"->101,"arrival" -> "Otopeni", "SFO" -> "San Fran")

    //简单的插入
    sc.makeRDD(Seq(numbers,airports)).saveToEs("hmh_index/simple_curd",Map("es.mapping.id"->"id"))
  }

  /**
    * 通过对象实例插入
    * @param sc
    */
  def objectInsert(sc : SparkContext): Unit ={
    //define a case class
    case class Trip(id:String,departure: String ,arrival: String)
    val upcomingTrip = Trip("1","OTP","SFO")
    val lastWeekTrip = Trip("2","MUC","OTP")
    val rdd = sc.makeRDD(Seq(upcomingTrip,lastWeekTrip))
    //通过对象实例插入
    EsSpark.saveToEs(rdd,"hmh_index/docs",Map("es.mapping.id"-> "id"))
  }

  /**
    * 插入json数据
    * @param sc
    */
  def jsonInsert(sc : SparkContext): Unit ={
    val json1 = """{"id":100,"reason":"business","airport":"SFO"}"""
    val json2 = """{"id":101,"participants":5,"airport":"OTP"}"""

    //插入json数据
    sc.makeRDD(Seq(json1,json2)).saveJsonToEs("spark/json-trips",Map("es.mapping.id"->"id"))
  }

  /**
    * 动态插入
    * @param sc
    */
  def dynamicInsert(sc : SparkContext): Unit ={
    val game = Map("media_type"->"game","title"->"FF VI","year"->"1992")
    val book = Map("media_type"->"book","title"->"Harry Potter","year"->"2010")
    val music = Map("media_type"->"music","title"->"Surfing With The Alien")

    //动态插入索引、类型
    sc.makeRDD(Seq(game,book,music)).saveToEs("spark/doc_{media_type}")
  }

  /**
    * 处理文档元数据 插入ID
    * @param sc
    */
  def metadateInsert(sc : SparkContext): Unit ={
    val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
    val muc = Map("iata" -> "MUC", "name" -> "Munich")
    val sfo = Map("iata" -> "SFO", "name" -> "San Fran")

    //处理文档元数据 插入ID
    var airportsRDD = sc.makeRDD(Seq((1,otp),(2,muc),(3,sfo)))
    airportsRDD.saveToEsWithMeta("spark/2015")
  }

  /**
    * 处理文档元数据 插入ID TTL VERSION
    * @param sc
    */
  def otherMeteInsert(sc : SparkContext): Unit ={
    val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
    val muc = Map("iata" -> "MUC", "name" -> "Munich")
    val sfo = Map("iata" -> "SFO", "name" -> "San Fran")

    val otpMeta = Map(ID -> 1, TTL -> "3h")
    val mucMeta = Map(ID -> 2,VERSION ->"35")  //此处多次运行时需要修改version值,变大,不然会报错
    val sfoMeta = Map(ID ->3)

    //处理文档元数据 插入ID TTL VERSION
    val airportsRDD = sc.makeRDD(Seq((otpMeta,otp),(mucMeta,muc),(sfoMeta,sfo)))
    airportsRDD.saveToEsWithMeta("spark/2015")
  }

  /**
    * 查询索引数据 循环遍历
    * @param sc
    */
  def selectAllDate(sc : SparkContext): Unit ={
    val sparkRDD = sc.esRDD("spark/2015")
    sparkRDD.foreachPartition(p => p.foreach(x=>print("all_value:"+x)))
  }

  /**
    * 带条件查询
    * @param sc
    */
  def queryIndexData(sc : SparkContext): Unit ={
    val queryRdd1 = sc.esRDD("spark/2015","?q=*O*")//查询所有字段
    val queryRdd2 = sc.esRDD("spark/2015","?q=iata:OT*") //查询某一个字段

    queryRdd1.foreachPartition(p=>p.foreach(x =>print("queryRdd1:"+x)))
    queryRdd2.foreachPartition(p=>p.foreach(x =>print("queryRdd2:"+x)))
  }

  /**
    * dsl语句查询
    * @param sc
    */
  def dslQueryIndexData(sc : SparkContext): Unit = {
    val query:String =s"""{
       "query": {
          "match": {
          "iata": "MUC"
          }
       }
    }"""
    val rdd = sc.esRDD("spark/2015", query)
    println("dsl query result ===: "+rdd.collect().toBuffer)
  }
}
