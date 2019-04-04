package com.es

/**
  * Created by Administrator on 2019/4/3.
  */
object Regex {

  def regexMatch(): Unit = {
    //regex email
    val emailRegex = "^[\\w-]+(\\.[\\w-]+)*@[\\w-]+(\\.[\\w-]+)+$".r
    for (matchString <- emailRegex.findAllIn("zhouzhihubeyond@sina.com")) {
      println(matchString)
    }

    //url网址
    val urlRegex = "^[a-zA-Z]+://(\\w+(-\\w+)*)(\\.(\\w+(-\\w+)*))*(\\?\\s*)?$".r
    for(matchString <- urlRegex.findAllIn("http://www.xuetuwuyou.com"))
    {
      println(matchString)
    }

    //电话号码
    val phoneRegex = "(86)*0*13\\d{9}".r
    for(matchString <- phoneRegex.findAllIn("13887888888"))
    {
      println(matchString)
    }

    //IP地址
    val ipRegex="(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)".r
    for(matchString <- ipRegex.findAllIn("192.168.1.165"))
    {
      println(matchString)
    }

    //提取器（Extractor）
    for(ipRegex(one,two,three,four) <- ipRegex.findAllIn("192.168.1.165"))
    {
      println("IP子段1:"+one)
      println("IP子段2:"+two)
      println("IP子段3:"+three)
      println("IP子段4:"+four)
    }

    //提取邮箱中的用户名
    val emailNameRegex="^([\\w-]+(\\.[\\w-]+)*)@[\\w-]+(\\.[\\w-]+)+$".r
    for(emailNameRegex(domainName,_*) <- emailNameRegex.findAllIn("zhouzhihubeyond@sina.com"))
    {
      println(domainName)
    }


  }

}