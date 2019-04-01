package com.es

import com.es.study.{DStream, SimpleCURD, SparkSQL}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
  * Created by Administrator on 2018/6/25.
  */
object EsToSpark {
  def main(args: Array[String]) {
//    val conf  = new SparkConf().setAppName("ESHadoopTest").setMaster("local")
//    //定义es各节点、端口、id为哪个字段
//    conf.set("es.nodes", "192.168.1.165")
//    conf.set("es.port", "9200")
    //    conf.set("es.mapping.id", "userid")
//    val sc = new SparkContext(conf)

    val conf = new SparkConf().setMaster("local").setAppName("ESHadoopTest")
      .set("es.index.auto.create", "true")
      .set("es.nodes", "192.168.1.165")
      .set("es.port", "9200")
//      .set("es.mapping.id", "userid")

    val sc = new SparkContext(conf)

    /**
      * 简单的增删改查
      */
    SimpleCURD.elasticSearchCURD(sc)

    /**
      * 处理DStream数据
      */
    DStream.dealDStream(sc)

    /**
      * 数据库保存到ES
      */
    SparkSQL.dealSparkSQL(sc)




//    /**
//      * 写入es
//      */
//    val person = Map("userid" -> "0005", "title" -> "测试例子5", "name" -> "Tom5","age" -> 25)
//    sc.makeRDD(Seq(person)).saveToEs("hmh_index/user")
//
//    /**
//      * 读取所有
//      */
//    val esAllRDD = sc.esRDD("hmh_index/user")
//    esAllRDD.foreachPartition(p => p.foreach(print))
//
//    /**
//      * 根据查询条件读取
//      */
//    val esQueryRDD = sc.esRDD("hmh_index/user","?q=Tom*")
//    val esQueryRDD1 = sc.esRDD("hmh_index/user","?q=name:Tom")
//
//    //打印输出
//    esQueryRDD.foreachPartition(p => p.foreach(print))
//    esQueryRDD1.foreachPartition(p => p.foreach(print))
//    sc.stop()
  }

}
