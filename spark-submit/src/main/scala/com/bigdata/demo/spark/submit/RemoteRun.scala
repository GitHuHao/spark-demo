package com.bigdata.demo.spark.submit

import org.apache.spark.{SparkConf, SparkContext}

object RemoteRun {

  def main(args: Array[String]): Unit = {
    // idea 远程提交 spark 集群运行，常用用于 spark 程序调试使用，代码修改，需要手动执行打包才会生效
    val conf = new SparkConf().setAppName(getClass.getSimpleName)
      .setMaster("spark://hadoop01:7077,hadoop02:7077")
      .setJars(List("./spark-submit/target/spark-submit.jar")) //  每次修改完毕，必须重新只执行打包
      .setIfMissing("spark.driver.host","192.168.2.101") // 设置本机 ip
    val sc = new SparkContext(conf)
    try {
      sc.textFile("hdfs://hadoop01:9000/apps/mr/wc/in/*")
        .flatMap(_.split("\\s+"))
        .map((_, 2))
        .reduceByKey(_ + _)
        .foreachPartition(it => println(it.mkString(","))) // spark web-ui
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      sc.stop()
    }
  }

}
