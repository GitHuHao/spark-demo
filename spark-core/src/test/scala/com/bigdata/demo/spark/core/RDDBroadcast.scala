package com.bigdata.demo.spark.core

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class RDDBroadcast {

  @Test
  def sparkBroadcast(): Unit = {
    val conf = new SparkConf().setAppName("spark-broadcast").setMaster("local[*]")
    val sc = new SparkContext(conf)

    try {
      val seq = Seq(Person("a1", 21), Person("a50", 70), Person("a90", 110))

      val start1 = System.currentTimeMillis()

      sc.parallelize((0 until 100).map(i => Person(s"a${i}", 20 + i)), 6)
        .filter(seq.contains(_)) // rdd 转换 或 执行算子 中 通过 xx.value 方式引用广播变量
        .foreachPartition(x => println(x.mkString(","))) // 不使用广播变量，清空下变量会被发射给所有任务，使用广播变量时，共享对象被发送给参与计算节点，通一个节点上 task 共享一份数据

      val end1 = System.currentTimeMillis()
      println(s"duration: ${end1-start1} mills")
      /**
       * Person(a90,110)
       * Person(a50,70)
       *
       * Person(a1,21)
       *
       * duration: 1133 mills
       */

      val broadcastedSeq = sc.broadcast[Seq[Person]](seq) // 声明广播变量(对象是可序列化类型)
      val start2 = System.currentTimeMillis()

      sc.parallelize((0 until 100).map(i => Person(s"a${i}", 20 + i)), 6)
        .filter(broadcastedSeq.value.contains(_)) // rdd 转换 或 执行算子 中 通过 xx.value 方式引用广播变量
        .foreachPartition(x => println(x.mkString(","))) // 不使用广播变量，清空下变量会被发射给所有任务，使用广播变量时，共享对象被发送给参与计算节点，通一个节点上 task 共享一份数据

      val end2 = System.currentTimeMillis()
      println(s"duration: ${end2-start2} mills")

      /**
       * Person(a90,110)
       * Person(a50,70)
       *
       *
       *
       * Person(a1,21)
       * duration: 25 mills
       */

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      sc.stop()
    }

  }


}
