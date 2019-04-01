package com.es
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark._

object AppTest{

  case class Trip(name:String)

  def main(args: Array[String]): Unit = {
    println( "Hello World!" )
//    System.setProperty("hadoop.home.dir", "G:\\hadoop_home")
//    val spark = SparkSession.builder()
//      .appName("SparkTest")
//      .master("local[5]")
//      .config("es.index.auto.create", "true")
//      .config("pushdown", "true")
//      .config("es.nodes", "192.168.1.165")
//      .config("es.port", "9200")
//      .config("es.nodes.wan.only", "true")
//      .getOrCreate()
    //从ES中读取数据
//    val sparkDF = spark.sqlContext.read.format("org.elasticsearch.spark.sql").load("spark/2015")
//    sparkDF.take(10).foreach(println(_))
//    import spark.implicits._
//    import org.apache.hadoop.fs._
//
//    val data = spark.read.textFile("E:\\embraceWorkSpace\\sparkAndEs\\testDate\\people\\people1.txt")
//    //写入到ES，一定要按照这个格式，因为这种格式才带有元数据信息，content就是ES中的列名
//    val rdd = data.rdd.map{
//      x =>Trip(x)
//    }
//
//    EsSpark.saveToEs(rdd, "spark/doc_people")
//    spark.stop()


    //下面是用DSL语言操作elasticsearch
    val conf = new SparkConf().setMaster("local").setAppName("ESHadoopTest")
      .set("es.index.auto.create", "true")
      .set("es.nodes", "192.168.1.165")
      .set("es.port", "9200")
    //      .set("es.mapping.id", "userid")

    val sc = new SparkContext(conf)

    val query:String =s"""{
       "query": {
          "match": {
          "iata": "MUC"
          }
       }
    }"""
    val rdd = sc.esRDD("spark/2015", query)
    println(rdd.collect().toBuffer)

  }
}
