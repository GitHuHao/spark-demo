package com.bigdata.demo.spark.submit

import org.apache.spark.{SparkConf, SparkContext}

object LocalRun {

  def main(args: Array[String]): Unit = {
    // local[*] 默认为本机物流核数
    // local[n] n 超过物理核数，为微核数
    // 初始化创建 rdd 的分区与 local[n]中的你一致
    val conf = new SparkConf().setAppName("rdd").setMaster("local[4]")
    val sc = new SparkContext(conf)
    try {
      sc.textFile("hdfs://hadoop01:9000/apps/mr/wc/in/*")
        .flatMap(_.split("\\s+"))
        .map((_, 2))
        .reduceByKey(_ + _)
        .foreachPartition(it => println(it.mkString(",")))
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      sc.stop()
    }
  }

}
