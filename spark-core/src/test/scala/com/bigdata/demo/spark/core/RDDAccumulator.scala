package com.bigdata.demo.spark.core

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

import scala.collection.JavaConversions._
import scala.collection.mutable.{HashSet => MSet}

class LogAccumulator extends AccumulatorV2[String,Set[String]] {

  private val set = new MSet[String]

  // 判断是否为空
  override def isZero: Boolean = set.isEmpty

  // 赋值
  override def copy(): AccumulatorV2[String, Set[String]] = {
    val newAcc = new LogAccumulator()
    newAcc.synchronized{
      newAcc.set.addAll(this.set)
    }
    newAcc
  }

  // 重置
  override def reset(): Unit = set.clear()

  // 分区内部添加
  override def add(v: String): Unit = set.add(v)

  // 分区间合并
  override def merge(other: AccumulatorV2[String, Set[String]]): Unit = {
    other match {
      case o:LogAccumulator => set.addAll(o.value) // 先匹配类型，再使用
    }
  }

  // 输出是需要转换为不可变集合
  override def value: Set[String] = set.toSet

}



class RDDAccumulator {

  /**
   * spark 自带累加器 （只写不读），收集算子执行完毕才能读
   * longAccumulator(name: String): LongAccumulator
   * collectionAccumulator[T](name: String): CollectionAccumulator[T]
   */
  @Test
  def sparkAcc(): Unit ={
    val conf = new SparkConf().setAppName("long-acc").setMaster("local[*]")
    val sc = new SparkContext(conf)

    try {
      val numAcc = sc.longAccumulator("long-acc")
      sc.parallelize(0 until 100, 4).foreachPartition { it =>
        it.foreach { x =>
          if (x % 2 == 0) {
            numAcc.add(1)
          }
        }
      }
      // 注：此处的 sum 时对个数求 sum
      println(s"name: ${numAcc.name.get}, sum: ${numAcc.sum}, count:${numAcc.count}, avg:${numAcc.avg}, value:${numAcc.value}")
      /**
       * name: long-acc, sum: 50, count:50, avg:1.0, value:50
       */

      val collAcc = sc.collectionAccumulator[Int]("collect-acc")
      sc.parallelize(0 until 10,4).foreachPartition{it=>
        it.foreach { x =>
          if (x % 2 == 0) {
            collAcc.add(x)
          }
        }
      }

      println(s"name: ${collAcc.name.get}, get: ${collAcc.value.toArray.mkString(",")}")
      /**
       * name: collect-acc, get: 8,6,2,4,0
       */

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      sc.stop()
    }
  }

  @Test
  def logAcc(): Unit ={
    val conf = new SparkConf().setAppName("long-acc").setMaster("local[*]")
    val sc = new SparkContext(conf)
    try {
      val acc = new LogAccumulator
      sc.register(acc,"logAcc")

      sc.parallelize(Seq("1","2a","3","4b","5","6","7cd","8","9"),3).foreach{x=>
        val pattern = """^-?(\d+)""" // 以负号开头，或不以负号开头的数字
        val bool = x.matches(pattern)
        if(bool){
          acc.add(x)
        }
      }
      println(s"name: ${acc.name.get}, sum: ${acc.value.mkString(",")}")

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      sc.stop()
    }
  }



}
