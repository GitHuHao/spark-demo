package com.bigdata.demo.spark.exec

import com.bigdata.demo.spark.model.CNDLog
import com.bigdata.demo.spark.util.CommonSuit
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{Partitioner, SparkConf, SparkContext}


object RDDExec {

  val logger = Logger.getLogger(getClass.getSimpleName)

  def main(args: Array[String]): Unit = {
    logger.debug(Level.OFF)
    cdnLogAnalysis()
  }

  def cdnLogAnalysis(): Unit ={
    val conf = new SparkConf().setAppName("cdn-log").setMaster("local[*]")
    val sc = new SparkContext(conf)

    try {
      logger.info(">>>> 准备cdn日志分析 ")
      val cndRdd = sc.textFile(CommonSuit.getFile("cdn/cdn.txt")).map{ line=>
        try {
          CNDLog(line)
        } catch {
          case e:Exception => {
            logger.error(e.getMessage)
            null
          }
        }
      }.filter(_!=null)

      cndRdd.cache()

      logger.info(">>>> 分钟级别 pv")
      val mimutePvPath = "spark-core/sink/pv/minute"
      CommonSuit.deleteLocalDir(mimutePvPath)
      cndRdd.map(cdn => (cdn.minuteDate, 1)).reduceByKey(_+_)
        .repartition(1)
        .sortBy(_._2,false)
        .saveAsTextFile(mimutePvPath)

      logger.info(">>>> 分钟级别 uv")
      val mimuteUvPath = "spark-core/sink/uv/minute"
      CommonSuit.deleteLocalDir(mimuteUvPath)
      cndRdd.map(cdn => (cdn.minuteDate, cdn.ip)).groupByKey(4).map{
        case (minuteDate,ips) => (minuteDate,ips.toSet.size)
      }.repartition(1)
        .sortBy(_._2,false)
        .saveAsTextFile(mimuteUvPath)

      logger.info(">>>> 小时级别 pv")
      val hourPvPath = "spark-core/sink/pv/hour"
      CommonSuit.deleteLocalDir(hourPvPath)
      cndRdd.map(cdn => (cdn.hourDate, 1)).reduceByKey(_ + _)
        .repartition(1)
        .sortBy(_._2,false)
        .saveAsTextFile(hourPvPath)

      logger.info(">>>> 小时级别 uv")
      val hourUvPath = "spark-core/sink/uv/hour"
      CommonSuit.deleteLocalDir(hourUvPath)
      cndRdd.map(cdn => (cdn.hourDate, cdn.ip)).groupByKey(4).map{
        case (hourDate,ips) => (hourDate,ips.toSet.size)
      }.repartition(1)
        .sortBy(_._2,false)
        .saveAsTextFile(hourUvPath)

      logger.info(">>>> 天级别 pv")
      val dayPvPath = "spark-core/sink/pv/day"
      CommonSuit.deleteLocalDir(dayPvPath)
      cndRdd.map(cdn => (cdn.dayDate, 1)).reduceByKey(_ + _)
        .repartition(1)
        .sortBy(_._2,false)
        .saveAsTextFile(dayPvPath)

      logger.info(">>>> 天级别 uv")
      val dayUvPath = "spark-core/sink/uv/day"
      CommonSuit.deleteLocalDir(dayUvPath)
      cndRdd.map(cdn => (cdn.dayDate, cdn.ip)).groupByKey(4).map{
        case (dayDate,ips) => (dayDate,ips.toSet.size)
      }.repartition(1)
        .sortBy(_._2,false)
        .saveAsTextFile(dayUvPath)

      logger.info(">>>> ip 出现次数")
      val ipCount = "spark-core/sink/ip/count"
      CommonSuit.deleteLocalDir(ipCount)
      cndRdd.map(x=>(x.ip,1))
        .reduceByKey(_+_)
        .repartition(1)
        .sortBy(_._2,false)
        .saveAsTextFile(ipCount)

      logger.info(">>>> 独立ip数 ")
      val ipTotal = "spark-core/sink/ip/total"
      CommonSuit.deleteLocalDir(ipTotal)
      cndRdd.map(x=>(x.ip,1))
        .distinct()
        .map(_=>(1,1))
        .reduceByKey(_+_)
        .repartition(1)
        .values
        .saveAsTextFile(ipTotal)

      logger.info(">>>> 准备视频独立 IP 统计 ")
      val videoIpPv = "spark-core/sink/video/pv"
      CommonSuit.deleteLocalDir(videoIpPv)
      val videoRdd = cndRdd.filter(_.url.endsWith(".mp4")).map{x=>
        val temp = x.url.split("/")
        x.url = temp(temp.size-1)
        x
      }
      videoRdd.cache()

      logger.info(">>>> 视频资源访问 ip-pv ")
      videoRdd.map(x=>(x.url,1))
        .reduceByKey(_+_)
        .sortBy(_._2,false)
        .repartition(1)
        .saveAsTextFile(videoIpPv)

      logger.info(">>>> 视频资源访问 ip-uv ")
      val videoIpUv = "spark-core/sink/video/uv"
      CommonSuit.deleteLocalDir(videoIpUv)
      videoRdd.map(x=>((x.url,x.ip),1))
        .distinct()
        .map(x=>(x._1._1,1))
        .reduceByKey(_+_)
        .sortBy(_._2,false)
        .repartition(1)
        .saveAsTextFile(videoIpUv)

      logger.info(">>>> 阻塞，释放videoRdd占用内存 ")
      videoRdd.unpersist(blocking = true)

      logger.info(">>>> 准备流量分布统计 ")
      val flowRdd = cndRdd.map(x=>(x.hourDate,x.trafficSize))
      flowRdd.cache()

      logger.info(">>>> 各小时访问流量分布 ")
      val hourFlow = "spark-core/sink/flow/hour"
      CommonSuit.deleteLocalDir(hourFlow)
      flowRdd.map{x=>
        (x._1.substring(11,13),x._2)
      }.reduceByKey(_+_)
        .map(x=>(x._1,x._2/1024/1024/1024)) // B->b->M->G
        .sortBy(_._2,false)
        .map(x=>(x._1,s"${x._2}G"))
        .repartition(1)
        .saveAsTextFile(hourFlow)

      logger.info(">>>> 每天各小时流量分布 ")
      val dateFlowDist = "spark-core/sink/flow/daily"
      CommonSuit.deleteLocalDir(dateFlowDist)

      val filteredFlow = flowRdd.reduceByKey(_+_)
        .map(x=>(s"${x._1} ${x._2/1024/1024/1024}",(x._1,x._2/1024/1024/1024))) // B->b->M->G
//        .map(x=>(s"${x._1} ${x._2/1024/1024}",(x._1,x._2/1024/1024))) // B->b->M (兆)
        .filter(_._2._2>0)

      filteredFlow.cache()

      logger.info(">>>> 计算日志涉及日期分布 ")
      val dates = filteredFlow.map(_._1.substring(0, 10)).distinct().collect()
      val dateCount = dates.size
      println(s"dateSize: ${dateCount}, series: ${dates.mkString(",")}")

      filteredFlow.repartitionAndSortWithinPartitions(
        new Partitioner {
          override def numPartitions: Int = dateCount
          override def getPartition(key: Any): Int = dates.indexOf(key.toString.substring(0,10))
        }
      ).map(x=>(x._2._1,s"${x._2._2}G"))
        .saveAsTextFile(dateFlowDist)

      filteredFlow.unpersist()

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      sc.stop()
    }
  }

}
