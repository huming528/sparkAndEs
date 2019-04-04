package com.es

import com.es.dataFrame.DataFrameAPI

/**
 * 程序主入口
 *
 */
object App{

  def main(args: Array[String]): Unit = {
    println("start test!")
    //读取文件内容
//    ScalaFileReader.readFile()

    //scala regex
//    Regex.regexMatch()

    //DataFrame
    //DataFrame对象上Action操作
//    DataFrameAPI.actionDataFrame()

    //DataFrame对象上的条件查询操作
//    DataFrameAPI.queryDataFrame()

    //针对DataFrame数据进行group by 操作
//    DataFrameAPI.groupbyDataFrame()

    //DataFrame对象上的join操作
    DataFrameAPI.joinDataFrame()
  }
}
