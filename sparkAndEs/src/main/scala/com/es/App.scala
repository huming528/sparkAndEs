package com.es
import com.es.study.{DStream, SimpleCURD, SparkSQL}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark._

/**
  * 工程入口
  * Created by Administrator on 2018/6/25.
  */
object AppTest{

  case class Trip(name:String)

  def main(args: Array[String]): Unit = {
    println( "start test!" )

    val conf = new SparkConf().setMaster("local").setAppName("ESHadoopTest")
      .set("es.index.auto.create", "true")
      .set("es.nodes", "192.168.1.165")
      .set("es.port", "9200")

    val sc = new SparkContext(conf)

    /**
      * 简单的增删改查
      */
    SimpleCURD.elasticSearchCURD(sc)

    /**
      * 处理DStream数据
      */
    DStream.dealDStream(sc)

  }
}
