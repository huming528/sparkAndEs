package com.es.dataFrame

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql._

/**
  * Created by Administrator on 2019/4/4.
  */
object DataFrameAPI {

  def createSparkSession() : SparkSession = {
    val ss = SparkSession.builder()
      .appName("scalaDataFrame")
      .master("local[4]")
      .config("es.index.auto.create", "true")
      .config("es.nodes", "192.168.1.165")
      .config("es.port", "9200")
      .config("es.mapping.date.rich","false")  //解决日期格式问题 以字符串格式处理
      .getOrCreate()

    ss
  }

  /**
    * 查询获取DataFrame
    * @return
    */
  def getDataFrame(): DataFrame = {
    var ss = createSparkSession()
    //查询获取DataFrame
    val sparkDataFrame = ss.esDF("patient_index/outp_mr")
    sparkDataFrame
  }

  /**
    * 带条件查询DataFrame
    * @return
    */
  def queryEsDataFrame(): DataFrame = {
    var ss = createSparkSession()
    ss.esDF("patient_index/outp_mr","?q=org_patient_id:0001196399")
  }
  /**
    * DataFrame对象上Action操作
    */
  def actionDataFrame(): Unit ={
    var esDFrame =getDataFrame()
    //show 只显示前20条记录。且过长的字符串会被截取
    println("show 只显示前20条记录。且过长的字符串会被截取")
    esDFrame.show()

    //show(numRows: Int)  显示numRows条
    println("show(numRows: Int)  显示numRows条")
    esDFrame.show(3)

    //show(truncate: Boolean)  是否截取20个字符，默认为true
    println("show(truncate: Boolean)  是否截取20个字符，默认为true")
    esDFrame.show(false)

    //show(numRows: Int, truncate: Int) 显示记录条数，以及截取字符个数，为0时表示不截取
    println("show(numRows: Int, truncate: Int) 显示记录条数，以及截取字符个数，为0时表示不截取")
    esDFrame.show(3,5)

    //带条件查询DataFrame
    var queryDFram = queryEsDataFrame()

    //collect：获取所有数据到数组
    //不同于前面的show方法，这里的collect方法会将jdbcDF中的所有数据都获取到，并返回一个Array对象
    var dFrameArr = queryDFram.collect()

    //array 操作
//    for(tempArr <- dFrameArr){
//      println("tempArr:"+ tempArr)
//    }

    //数组的遍历
    for(i <- 0 to dFrameArr.length-1){
      println("第"+i+"条数据值:"+ dFrameArr(i))
    }

    //collectAsList：获取所有数据到List
    var dFrameList = queryDFram.collectAsList()
    println("dFrameList 1 :"+dFrameList.get(1))

    //遍历List
    for(i <- 0 to dFrameList.size() -1 ) println("dFrameList 第" + i+"行数据的值:" + dFrameList.get(i))

    //describe(cols: String*)：获取指定字段的统计信息
//    queryDFram.describe("dept_id","dept_name","patient_name").show()
    queryDFram.describe("age_year").show()


    //first, head, take, takeAsList：获取若干行记录
//        （1）first获取第一行记录
//    　　（2）head获取第一行记录，head(n: Int)获取前n行记录
//    　　（3）take(n: Int)获取前n行数据
//    　　（4）takeAsList(n: Int)获取前n行数据，并以List的形式展现
//    　　以Row或者Array[Row]的形式返回一行或多行数据。first和head功能相同。
//    　　take和takeAsList方法会将获得到的数据返回到Driver端，所以，使用这两个方法时需要注意数据量，以免Driver发生OutOfMemoryErro
    println("queryDFram.first()"+queryDFram.first())
    println("queryDFram.head()" + queryDFram.head())
    println("queryDFram.take(3)" + queryDFram.head())
  }

  /**
    * DataFrame对象上的条件查询等操作
    */
  def queryDataFrame(): Unit ={
    //where条件相关 全量数据10180条
    var esDFrame =getDataFrame()

    //传入筛选条件表达式，可以用and和or。得到DataFrame类型的返回结果
    var whereResult = esDFrame.where("org_patient_id= '0001196399'")
    whereResult.show(10)

    //filter：根据字段进行筛选
    //传入筛选条件表达式，得到DataFrame类型的返回结果。和where使用条件相同
    esDFrame.filter("org_patient_id= '0001196399'").show()

    //查询指定字段
    esDFrame.select("dept_id","dept_name","patient_name").show()

    //还有一个重载的select方法，不是传入String类型参数，而是传入Column类型参数。可以实现select id, id+1 from test这种逻辑
    esDFrame.select(esDFrame("patient_name"),esDFrame("age_year")+1).show(false)

    //selectExpr：可以对指定字段进行特殊处理  可以直接对指定字段调用UDF函数，或者指定别名等。传入String类型参数，得到DataFrame对象
    esDFrame.selectExpr("patient_name as renmin ","round(age_year) as age").show(3)

    //col：获取指定字段 只能获取一个字段，返回对象为Column类型
    val col = esDFrame.col("age_year")

    //drop：去除指定字段，保留其他字段 返回一个新的DataFrame对象，其中不包含去除的字段，一次只能去除一个字段
    esDFrame.drop("sex_name").show()

    //limit方法获取指定DataFrame的前n行记录，得到一个新的DataFrame对象。和take与head不同的是，limit方法不是Action操作
    esDFrame.limit(5).show()

    //orderBy和sort：按指定字段排序，默认为升序 ,按指定字段排序。加个-表示降序排序。sort和orderBy使用方法相同 只能对数字类型和日期类型生效
    esDFrame.orderBy(esDFrame("age_year").desc).show(false)
    esDFrame.orderBy(-esDFrame("patient_birthday")).show(false)
  }

  /**
    * 针对DataFrame数据进行group by 操作
    */
  def groupbyDataFrame(): Unit ={
    //全量数据10180条
    var esDFrame =getDataFrame()

    //groupBy：根据字段进行group by操作 每个部门的平均年龄
    esDFrame.groupBy("dept_name").avg("age_year").show()
    println("上面计算每个部门对应的平均年龄")

    //统计每个部门的人数
    esDFrame.groupBy("dept_name").count().show()
    println("上面是统计每个部门的人数")

    //实现cube、rollup
    esDFrame.cube("dept_id","dept_name").avg("age_year").show()
    esDFrame.rollup("dept_id","dept_name").avg("age_year").show()

    //distinct 操作
    esDFrame.distinct().show()
    println("上面是distinct操作")

    esDFrame.dropDuplicates("dept_id","dept_name").show()
    println("上面是distinct \"dept_id\",\"dept_name\" 操作")

    //聚合 测试下面两种方式的结果相同
    esDFrame.groupBy("dept_name").sum("age_year").show()
    println("上面是groupBy sum操作")

    esDFrame.groupBy("dept_name").agg("age_year" -> "sum").show()
    println("上面是groupBy agg操作")

  }

  /**
    * DataFrame对象上的join操作
    */
  def joinDataFrame(): Unit ={
    //全量数据10180条
    var esDFrame =getDataFrame()

    //方便测试 生成两个简单的DataFrame
    var df1 = esDFrame.groupBy("dept_id","dept_name").avg("age_year")

    var df2 = esDFrame.groupBy("dept_id","sex_name").count()

    df1.show()
    df2.show()

    println("df1的数据量："+df1.count() +"   df2的数据量："+ df2.count())

    //笛卡尔积  下面这种方式会报错 实际业务中不会出现类似业务要求
//    df1.join(df2).show()
//    println("笛卡尔积：上面是df1.join(df2)的结果集，总数据量有: "+ df1.join(df2).count() +"条")

    //using一个字段形式
    df1.join(df2,"dept_id").show()
    println("using一个字段形式：上面是df1.join(df2,\"dept_id\")的结果集，总数据量有: "+ df1.join(df2,"dept_id") +"条")

    //指定join类型
    df1.join(df2,Seq("dept_id"),"inner").show()

    //在指定join字段同时指定join类型
    df1.join(df2,df1("dept_id") === df2("dept_id"),"inner").show()

  }


}
