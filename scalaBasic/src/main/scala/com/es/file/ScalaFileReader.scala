package com.es.file

import scala.io.Source

/**
  * 文件读取打印
  * Created by Administrator on 2019/4/3.
  */
object ScalaFileReader {

  def readFile(): Unit = {
    //读取文件
    var file = Source.fromFile("E:\\embraceWorkSpace\\scalaBasic\\testFile\\people.txt");

    //返回Iterator[String]
    var lineValue = file.getLines()

    //按行打印文件内容
    for(lv <- lineValue) println("lineValue:"+lv)

    //关闭文件
    file.close()

  }

}
