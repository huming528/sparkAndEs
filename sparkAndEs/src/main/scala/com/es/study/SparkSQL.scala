package com.es.study

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Encoders, Row, SQLContext, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL
import org.elasticsearch.spark.streaming.EsSparkStreaming

import scala.collection.mutable

/**
  * Created by Administrator on 2019/3/27.
  */
object SparkSQL {

  def dealSparkSQL(sc:SparkContext): Unit ={
    val spark = SparkSession.builder().appName("EsStreamingExample").getOrCreate()
    case class Person(name:String,surname:String,age:Int)

    val file = spark.read.textFile("E:\\embraceWorkSpace\\sparkAndEs\\testDate\\people\\people1.txt")
//    val file = sc.textFile("E:\\embraceWorkSpace\\sparkAndEs\\testDate\\people\\people1.txt")
//    implicit  val matchError =  org.apache.spark.sql.Encoders.tuple( Encoders.STRING, Encoders.STRING, Encoders.INT)
    implicit val matchError = org.apache.spark.sql.Encoders.kryo[Row]
    val ds = file.map(line => {
      val arr = line.split(",")
      Row.fromSeq(arr.toSeq)
    })

    //创建表头
    val fields = "name,surnme,age"
    val field_array = fields.split(",")
    val schema = StructType(field_array.map(fieldName => StructField(fieldName, StringType, true)))

    //创建DataFrame
    val df = spark.createDataFrame(ds.rdd, schema)


    JavaEsSparkSQL.saveToEs(df, "spark/people")

//    val ssc = new StreamingContext(sc,Seconds(1))
//        var microbatches = mutable.Queue(ds.rdd)
//        var dStream = ssc.queueStream(microbatches)
//
//        EsSparkStreaming.saveToEs(dStream,"spark/doc_people")
//    ssc.start()
//    ssc.awaitTermination()


//    df.writeStream
//          .option("checkpointLocation","E:\\embraceWorkSpace\\sparkAndEs\\logs")
//          .format("es")
//          .start("spark/people")

//
//    import spark.implicits._
//    val peopleArray = spark.readStream
//      .textFile("E:\\embraceWorkSpace\\sparkAndEs\\testDate\\people\\people1.txt")
//      .map(_.split(","))
//
//    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
//    peopleArray.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
//
//    val people = peopleArray.map(p=> Person(p(0),p(1),p(2).trim.toInt))
//
//    people.writeStream
//      .option("checkpointLocation","E:\\embraceWorkSpace\\sparkAndEs\\logs")
//      .format("es")
//      .start("spark/people")


  }

}
