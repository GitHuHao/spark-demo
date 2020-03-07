package com.bigdata.demo.spark.streaming

import breeze.linalg.min
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.junit.Test

import scala.collection.mutable.SynchronizedQueue

class DistinctAction {

  /**
   * qStream.transform ((rdd, time) =>rdd.map((_, time))).print() 直接按批间隔打印输出
   * -------------------------------------------
   * Time: 1583417500000 ms
   * -------------------------------------------
   * (a0,1583417500000 ms)
   *
   * -------------------------------------------
   * Time: 1583417501000 ms
   * -------------------------------------------
   * (a0,1583417501000 ms)
   * (a0,1583417501000 ms)
   * (a1,1583417501000 ms)
   *
   * -------------------------------------------
   * Time: 1583417502000 ms
   * -------------------------------------------
   * (a0,1583417502000 ms)
   * (a0,1583417502000 ms)
   * (a1,1583417502000 ms)
   * (a0,1583417502000 ms)
   * (a1,1583417502000 ms)
   * (a2,1583417502000 ms)
   *
   * 》》》》》》》》》》》》》》》》》》》》》》》》》》
   *
   * qStream.transform ((rdd, time) =>rdd.map((_, time)))
   * .reduceByKeyAndWindow((t1:Time,t2:Time)=> if(t1.less(t2)) t1 else t2, batchDuration * 2, batchDuration).print()
   * 1.滑动间隔为批间隔的2倍，窗口为批间隔三倍；
   * 2.每滑动一次进行一次计算；
   * 3.每次计算窗口中，key 相同的，取较早的
   * -------------------------------------------
   * Time: 1583417623000 ms
   * -------------------------------------------
   * (a0,1583417623000 ms)
   *
   * -------------------------------------------
   * Time: 1583417624000 ms
   * -------------------------------------------
   * (a1,1583417624000 ms)
   * (a0,1583417623000 ms)
   *
   * -------------------------------------------
   * Time: 1583417625000 ms
   * -------------------------------------------
   * (a1,1583417624000 ms)
   * (a2,1583417625000 ms)
   * (a0,1583417624000 ms)
   *
   * -------------------------------------------
   * Time: 1583417626000 ms
   * -------------------------------------------
   * (a1,1583417625000 ms)
   * (a2,1583417625000 ms)
   * (a3,1583417626000 ms)
   * (a0,1583417625000 ms)
   *
   * 》》》》》》》》》》》》》》》》》》》》》》》》》》
   *
   * qStream.transform ((rdd, time) =>rdd.map((_, time)))
   * .reduceByKeyAndWindow((t1:Time,t2:Time)=> if(t1.less(t2)) t1 else t2, batchDuration * 2, batchDuration)
   * .transform((rdd,time) => rdd.filter(t=> t._2.greaterEq(time)))
   * .print()
   * 1.滑动间隔为批间隔两倍，窗口长度为批间隔3倍
   * 2.每滑动一次，触发一次计算
   * 3.第一次transform 是给各批的rdd 添加上其对应批的时间维度
   * 4.reduceByKeyAndWindow 此计算是在自定的滑动步长和窗口长度上进行的规约操作，目的是在窗口级别，去重（只保留最早的元素）
   * 5.第二次 transform 中的 (rdd,time) 其中time 是当前批的时间戳，后面的filter操作是过滤掉同窗口中出当前最新批以外其余批的元素
   *
   * -------------------------------------------
   * Time: 1583417939000 ms
   * -------------------------------------------
   * (a0,1583417939000 ms)
   *
   * -------------------------------------------
   * Time: 1583417940000 ms
   * -------------------------------------------
   * (a1,1583417940000 ms)
   *
   * -------------------------------------------
   * Time: 1583417941000 ms
   * -------------------------------------------
   * (a2,1583417941000 ms)
   *
   * -------------------------------------------
   * Time: 1583417942000 ms
   * -------------------------------------------
   * (a3,1583417942000 ms)
   *
   * -------------------------------------------
   * Time: 1583417943000 ms
   * -------------------------------------------
   * (a4,1583417943000 ms)
   *
   * -------------------------------------------
   * Time: 1583417944000 ms
   * -------------------------------------------
   * (a5,1583417944000 ms)
   */
  @Test
  def distinctByWindowAndTime(): Unit ={
    val conf = new SparkConf().setMaster("local[*]").setAppName("distinctByWindowAndTime")
    val batchDuration = Seconds(1)
    val ssc = new StreamingContext(conf,batchDuration)

    val queue = new SynchronizedQueue[RDD[String]]()
    val qStream = ssc.queueStream(queue)

    qStream.transform ((rdd, time) =>rdd.map((_, time))) // 每批包含若干rdd，此处是将同批rdd 添加 对应批时间戳，
      .reduceByKeyAndWindow((t1:Time,t2:Time)=> if(t1.less(t2)) t1 else t2, batchDuration * 2, batchDuration) // 在窗口级别执行去重操作，只保留较早的元素(默认Key相同元素，时间维度肯定不同)
      .transform((rdd,time) => rdd.filter(t=> t._2.greaterEq(time))) // 同窗口中，只保留当前最新批的数据，滑动步长套出来的之前批数据，默认已经处理，直接过滤掉
      .print()

    for (i <- 0 until 6) {
      queue += ssc.sparkContext.parallelize((0 to i)).flatMap(x => (0 to x).map(y => (s"a${y}")))
      Thread.sleep(1000);
    }

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * qStream.print()
   * 直接按批收，每批包含若干rdd
   * -------------------------------------------
   * Time: 1583420518000 ms
   * -------------------------------------------
   * a0
   *
   * -------------------------------------------
   * Time: 1583420519000 ms
   * -------------------------------------------
   * a0
   * a0
   * a1
   *
   * -------------------------------------------
   * Time: 1583420520000 ms
   * -------------------------------------------
   * a0
   * a0
   * a1
   * a0
   * a1
   * a2
   *
   * 》》》》》》》》》》》》》》》》》》》》》》》》》》
   * 保留上批次rdd，用于下批次计算取差集，只保留最新的元素，实现相邻批次间去重效果
   * qStream
   * .transform{rdd =>
   *    val value = rdd.subtract(lastRDD)
   *    lastRDD = rdd
   *    value
   * }
   * .print()
   * -------------------------------------------
   * Time: 1583420646000 ms
   * -------------------------------------------
   * a0
   *
   * -------------------------------------------
   * Time: 1583420647000 ms
   * -------------------------------------------
   * a1
   *
   * -------------------------------------------
   * Time: 1583420648000 ms
   * -------------------------------------------
   * a2
   *
   * -------------------------------------------
   * Time: 1583420649000 ms
   * -------------------------------------------
   * a3
   *
   * -------------------------------------------
   * Time: 1583420650000 ms
   * -------------------------------------------
   * a4
   *
   * -------------------------------------------
   * Time: 1583420651000 ms
   * -------------------------------------------
   * a5
   *
   */
  @Test
  def distinctByRememberAndSubstract(): Unit ={
    val conf = new SparkConf().setMaster("local[*]").setAppName("distinctByWindowAndTime")
    val batchDuration = Seconds(1)
    val ssc = new StreamingContext(conf,batchDuration)

//    ssc.remember(batchDuration*2)

    val queue = new SynchronizedQueue[RDD[String]]()
    val qStream = ssc.queueStream(queue)

    var lastRDD:RDD[String] = ssc.sparkContext.emptyRDD;

    qStream
      .transform{rdd =>
        println(lastRDD.collect().toList)
        val value = rdd.subtract(lastRDD)
        lastRDD = rdd
        value
      }
      .print()

    for (i <- 0 until 6) {
      queue += ssc.sparkContext.parallelize((0 to i)).flatMap(x => (0 to x).map(y => (s"a${y}")))
      Thread.sleep(1000);
    }

    ssc.start()
    ssc.awaitTermination()

    sys.addShutdownHook(println(lastRDD.collect().toList))

  }

  /**
   *  qStream.print()
   *  批次间隔内，存在重复
   * -------------------------------------------
   * Time: 1583421197000 ms
   * -------------------------------------------
   * a0
   *
   * -------------------------------------------
   * Time: 1583421198000 ms
   * -------------------------------------------
   * a0
   * a0
   * a1
   *
   * -------------------------------------------
   * Time: 1583421199000 ms
   * -------------------------------------------
   * a0
   * a0
   * a1
   * a0
   * a1
   * a2
   *
   * 》》》》》》》》》》》》》》》》》》》》》》》》》》
   * 保持上个批次全量rdd, 将本批次rdd 与上批次left join，关联不上的就是新的，直接输出 （与substract 相比，需要频繁GC，可能不太稳定）
   * var lastRDD:RDD[(String,Time)] = ssc.sparkContext.emptyRDD;
   * qStream
   * .transform{(rdd,time) =>
   *    val nowRDD = rdd.map((_,time))
   *    val value = nowRDD.leftOuterJoin(lastRDD).filter(_._2._2.isEmpty).map{case (k,(v1,v2)) => (k,v1)}
   *    lastRDD = nowRDD
   *    value
   * }
   * .print()
   *
   * -------------------------------------------
   * Time: 1583421263000 ms
   * -------------------------------------------
   * (a0,1583421263000 ms)
   *
   * -------------------------------------------
   * Time: 1583421264000 ms
   * -------------------------------------------
   * (a1,1583421264000 ms)
   *
   * -------------------------------------------
   * Time: 1583421265000 ms
   * -------------------------------------------
   * (a2,1583421265000 ms)
   *
   * -------------------------------------------
   * Time: 1583421266000 ms
   * -------------------------------------------
   * (a3,1583421266000 ms)
   *
   * -------------------------------------------
   * Time: 1583421267000 ms
   * -------------------------------------------
   * (a4,1583421267000 ms)
   *
   * -------------------------------------------
   * Time: 1583421268000 ms
   * -------------------------------------------
   * (a5,1583421268000 ms)
   */
  @Test
  def distinctByRememberAndJoin(): Unit ={
    val conf = new SparkConf().setMaster("local[*]").setAppName("distinctByWindowAndTime")
    val batchDuration = Seconds(1)
    val ssc = new StreamingContext(conf,batchDuration)

    //    ssc.remember(batchDuration*2)

    val queue = new SynchronizedQueue[RDD[String]]()
    val qStream = ssc.queueStream(queue)

    var lastRDD:RDD[(String,Time)] = ssc.sparkContext.emptyRDD;

    qStream
      .transform{(rdd,time) =>
        val nowRDD = rdd.map((_,time))
        val value = nowRDD.leftOuterJoin(lastRDD).filter(_._2._2.isEmpty).map{case (k,(v1,v2)) => (k,v1)}
        lastRDD = nowRDD
        value
      }
      .print()

    for (i <- 0 until 6) {
      queue += ssc.sparkContext.parallelize((0 to i)).flatMap(x => (0 to x).map(y => (s"a${y}")))
      Thread.sleep(1000);
    }

    ssc.start()
    ssc.awaitTermination()
  }


}
