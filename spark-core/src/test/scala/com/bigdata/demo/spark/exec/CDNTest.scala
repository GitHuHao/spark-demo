package com.bigdata.demo.spark.exec

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalField
import java.util.Locale

import org.junit.Test

class CDNTest {

  @Test
  def textParse(): Unit ={
    val text = """117.166.17.24 HIT 0.014 [15/Feb/2017:19:49:46 +0800] "GET https://v-cdn.abc.com.cn/videojs/video-js.css HTTP/2.0" 200 16328 "https://v.abc.com.cn/video/iframe/player.html?id=140876&autoPlay=1" "Mozilla/5.0 (Linux; Android 6.0; HUAWEI NXT-AL10 Build/HUAWEINXT-AL10; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/53.0.2785.49 Mobile MQQBrowser/6.2 TBS/043024 Safari/537.36 MicroMessenger/6.5.4.1000 NetType/WIFI Language/zh_CN""""
    val pattern = """(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) (\w{3,4}) (.+) \[(.+) (.+)\] \"(\w+) (.+) (.+)\" (\d+) (\d+) \"(.*)\" \"(.*)\"""".r
    val pattern(ip, hitOrMiss, respTime, textTime, zone, method, url, protocol, statusCode, trafficSize, referer, userAgent) = text
    println(s"ip:${ip}")
    println(s"hitOrMiss:${hitOrMiss}")
    println(s"respTime:${respTime}")
    println(s"textTime:${textTime}")
    println(s"zone:${zone}")
    println(s"method:${method}")
    println(s"url:${url}")
    println(s"protocol:${protocol}")
    println(s"statusCode:${statusCode}")
    println(s"trafficSize:${trafficSize}")
    println(s"referer:${referer}")
    println(s"userAgent:${userAgent}")
  }

  @Test
  def timeFormat(): Unit ={
    val time = "15/Feb/2017:19:49:46 +0800"

    val locale = new Locale("en")
    val toDateFormat = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z",locale)
    val zonedDateTime = ZonedDateTime.parse(time,toDateFormat)

    val minuteStrFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:00 Z",locale)
    val hourStrFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:00:00 Z",locale)
    val dayStrFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd",locale)
    val monthStrFormat = DateTimeFormatter.ofPattern("yyyy-MM",locale)
    val yearStrFormat = DateTimeFormatter.ofPattern("yyyy",locale)

    println(zonedDateTime.format(minuteStrFormat))
    println(zonedDateTime.format(hourStrFormat))
    println(zonedDateTime.format(dayStrFormat))
    println(zonedDateTime.format(monthStrFormat))
    println(zonedDateTime.format(yearStrFormat))
  }







}
