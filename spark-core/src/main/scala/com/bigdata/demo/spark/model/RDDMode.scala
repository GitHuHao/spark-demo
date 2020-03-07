package com.bigdata.demo.spark.model

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale

/**
 * IP 命中率 响应时间 请求时间 请求方法 请求URL    请求协议 状态吗 响应大小 referer 用户代理
 * ClientIP Hit/Miss ResponseTime [Time Zone] Method URL Protocol StatusCode TrafficSize Referer UserAgent
 * 112.17.241.155 HIT 2537 [15/Feb/2017:00:22:45 +0800] "GET http://cdn.v.abc.com.cn/140987.mp4 HTTP/1.1" 206 12785299 "-" "Lavf/57.40.101"
 */

case class CNDLog(ip:String,
                  hitOrMiss:String,
                  respTime:Float,
                  textTime:String,
                  zone:String,
                  method:String,
                  var url:String,
                  protocol:String,
                  statusCode:Int,
                  trafficSize:Long,
                  referer:String,
                  userAgent:String,
                  dateTime: ZonedDateTime,
                  minuteDate:String,
                  hourDate:String,
                  dayDate:String,
                  monthDate:String,
                  yarnDate:String
                 ) extends Ordered[CNDLog] with Serializable{

  override def compare(that: CNDLog): Int = {
    this.hashCode().compareTo(that.hashCode())
  }
}

object CNDLog{
  val pattern = """(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) (\w+) (.+) \[(.+) (.+)\] \"(\w+) (.+) (.+)\" (\d+) (\d+) \"(.*)\" \"(.*)\"""".r
  val locale = new Locale("en")
  val toDateFormat = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z",locale)
  val minuteStrFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:00 Z",locale)
  val hourStrFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:00:00 Z",locale)
  val dayStrFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd",locale)
  val monthStrFormat = DateTimeFormatter.ofPattern("yyyy-MM",locale)
  val yearStrFormat = DateTimeFormatter.ofPattern("yyyy",locale)

  def apply(text: String): CNDLog = parse(text)

  def parse(text: String): CNDLog ={
    val pattern(ip, hitOrMiss, respTime, textTime, zone, method, url, protocol, statusCode, trafficSize, referer, userAgent) = text
    val dateTime = ZonedDateTime.parse(s"${textTime} ${zone}",toDateFormat)
    dateTime.getZone
    val minuteDate = dateTime.format(minuteStrFormat)
    val hourDate = dateTime.format(hourStrFormat)
    val dayDate = dateTime.format(dayStrFormat)
    val monthDate = dateTime.format(monthStrFormat)
    val yarnDate = dateTime.format(yearStrFormat)

    new CNDLog(ip, hitOrMiss, respTime.toFloat, textTime, zone, method, url, protocol, statusCode.toInt,
      trafficSize.toLong, referer, userAgent,dateTime,minuteDate,hourDate,dayDate,monthDate,yarnDate)
  }

}
