package com.bigdata.demo.spark.streaming

import java.io.File
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream}
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext, Time}
import org.junit.Test

import scala.collection.mutable
import scala.collection.mutable.SynchronizedQueue

object SparkSessionSingleton {

  @transient private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}

class DStreamInfo {

  /**
   * socketTextStream 读取行
   * 启动 server: nc -lk 9999
   * 启动 client: nc localhost 9999
   *
   * 先启动接收端 telnet localhost 9999
   * 再启动发送端 nc -l 9999
   *
   * socketTextStream 底层调用的其实是 socketStream
   * socketStream[String](hostname, port, SocketReceiver.bytesToLines, storageLevel)
   *
   */
  @Test
  def socketTextStream(): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
      .set("spark.streaming.stopGracefullyOnShutdown", "true") // 注册优雅停机钩子
    val ssc = new StreamingContext(conf, Seconds(1)) // 窗口大小
    try {
      ssc.socketTextStream("localhost", 9999)
        .flatMap(_.split(" "))
        .map((_, 1))
        .reduceByKey(_ + _)
        .print()
      ssc.start()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      ssc.awaitTermination()
    }
  }

  /**
   * textFileStream
   * 监听 hdfs目录，扫描启动 app 之后放入进去文件，按行读取输出，已经被处理的文件
   * 即便移出后移出入，或重命名 依旧不能被重复监听
   */
  @Test
  def textFileStream(): Unit = {
    val conf = new SparkConf().setAppName("streaming").setMaster("local[*]")
      .set("spark.streaming.stopGracefullyOnShutdown", "true") // 注册优雅停机钩子
    val ssc = new StreamingContext(conf, Seconds(1))
    try {
      ssc.textFileStream("hdfs://hadoop01:9000/apps/mr/wc/in")
        .flatMap(_.split("\\s+"))
        .map((_, 1))
        .reduceByKey(_ + _)
        .print()
      ssc.start()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      ssc.awaitTermination()
    }
  }

  /**
   * 自定义接收器
   * 从 socket 接收
   */
  @Test
  def receiveStream(): Unit = {
    val conf = new SparkConf().setAppName("socker").setMaster("local[*]")
      .set("spark.streaming.stopGracefullyOnShutdown", "true") // 注册优雅停机钩子
    val ssc = new StreamingContext(conf, Seconds(1))
    try {
      ssc.receiverStream(new SocketReceiver("localhost", 9999))
        .flatMap(_.split("\\s+"))
        .map((_, 1))
        .reduceByKey(_ + _)
        .print()

      ssc.start()

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      ssc.awaitTermination()
    }
  }

  /**
   * 从 rdd-queue 队列，接收输入，然后参与计算
   */
  @Test
  def queueStream(): Unit = {
    val conf = new SparkConf().setAppName("queue").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(1))
    try {
      val rddQueue = new SynchronizedQueue[RDD[Int]]()
      val queuedStream = ssc.queueStream(rddQueue)

      queuedStream.map(x => (x % 10, 1))
        .reduceByKey(_ + _)
        .print()

      ssc.start()

      for (_ <- 0 until 4) {
        rddQueue += ssc.sparkContext.makeRDD(1 to 30, 10)
        Thread.sleep(2000)
      }

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      ssc.awaitTermination()
    }
  }

  /**
   * 一对一
   * def map[U: ClassTag](mapFunc: T => U): DStream[U]
   */
  @Test
  def map(): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming-map")
    val batchDuration = Seconds(1)
    val ssc = new StreamingContext(conf, batchDuration)
    ssc.socketTextStream("localhost", 4000)
      .map(_.toUpperCase)
      .print()
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 一对多
   * def flatMap[U: ClassTag](flatMapFunc: T => TraversableOnce[U]): DStream[U
   */
  @Test
  def flatMap(): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming-map")
    val batchDuration = Seconds(1)
    val ssc = new StreamingContext(conf, batchDuration)
    ssc.socketTextStream("localhost", 4000)
      .flatMap(_.split("\\s+"))
      .print()
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 按指定条件过滤
   * def filter(filterFunc: T => Boolean): DStream[T]
   */
  @Test
  def filter(): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming-map")
    val batchDuration = Seconds(1)
    val ssc = new StreamingContext(conf, batchDuration)
    ssc.socketTextStream("localhost", 4000)
      .filter(_.startsWith("a"))
      .print()
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 按指定条件过滤
   * def repartition(numPartitions: Int): DStream[T]
   *
   * setMaster("local[5]") 使用5个核，默认分配一个给接收器，剩余4个充当worker
   *
   * 无论输出是RDD[U]还是 RDD[(K,V)]，各分区都是挨个接收的
   *
   * socket一次接收一个分区，无如输入则无输出
   * repartition流，分区数与设定一致，即使无接收也会执行打印操作
   * --- rddPartitions: , rddData:
   * >>> rddPartitions: 0,1,2,3,4,5, rddData:
   *
   * --- rddPartitions: 0,1,2,3,4,5,6,7,8,9,10,11,12,13, rddData: (a,1),(a,1),(a,1),(a,1),(a,1),(a,1),(a,1),(a,1),(a,1),(a,1),(a,1),(a,1),(a,1),(a,1)
   * >>> rddPartitions: 0,1,2,3,4,5, rddData: (a,1),(a,1),(a,1),(a,1),(a,1),(a,1),(a,1),(a,1),(a,1),(a,1),(a,1),(a,1),(a,1),(a,1)
   *
   */
  @Test
  def repartition(): Unit = {
    val conf = new SparkConf().setMaster("local[5]").setAppName("streaming-map")
    val batchDuration = Seconds(5)
    val ssc = new StreamingContext(conf, batchDuration)

    val socketStream = ssc.socketTextStream("localhost", 4000)
      .flatMap(_.split("\\s+"))
      .map((_, 1))

    socketStream.foreachRDD { rdd =>
      val rddPartitions = rdd.partitions.map(_.index).mkString(",")
      val rddData = rdd.collect().mkString(",")
      println(s"--- rddPartitions: ${rddPartitions}, rddData: ${rddData}")
    }

    val repartitionStream = socketStream.repartition(6)
    repartitionStream.foreachRDD { rdd =>
      val rddPartitions = rdd.partitions.map(_.index).mkString(",")
      val rddData = rdd.collect().mkString(",")
      println(s">>> rddPartitions: ${rddPartitions}, rddData: ${rddData}")
    }

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 合并两个stream
   * def union(that: DStream[T]): DStream[T]
   * --- c
   * --- d
   * --- b
   * --- a
   * ---
   * ---
   * --- A
   * --- B
   * --- D
   * --- C
   * >>> a
   * >>> c,d
   * >>> b,A,B,C,D
   *
   * repartition 之前一个元素一个分区，执行重分区后 一个分区一次可以处理多个元素
   */
  @Test
  def union(): Unit = {
    val conf = new SparkConf().setMaster("local[5]").setAppName("streaming-union")
    val batchDuration = Seconds(1)
    val ssc = new StreamingContext(conf, batchDuration)

    val syncQueue1 = new mutable.SynchronizedQueue[RDD[String]]()
    val syncQueue2 = new mutable.SynchronizedQueue[RDD[String]]()

    val qStream1 = ssc.queueStream(syncQueue1)
    val qStream2 = ssc.queueStream(syncQueue2)

    //    ssc.union(Seq(qStream1,qStream2))
    val unionedStream = qStream1.union(qStream2)

    unionedStream.foreachRDD { rdd =>
      rdd.foreachPartition { case iter =>
        println(s"--- ${iter.mkString(",")}")
      }
    }

    unionedStream.repartition(3).foreachRDD { rdd =>
      rdd.foreachPartition { case iter =>
        println(s">>> ${iter.mkString(",")}")
      }
    }

    syncQueue1 += ssc.sparkContext.makeRDD(Seq("a", "b", "c", "d"))
    syncQueue2 += ssc.sparkContext.makeRDD(Seq("A", "B", "C", "D"))

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 统计个stream 中 各 rdd  的原个数
   * def count(): DStream[Long]
   * 2
   * *
   * 3
   * *
   * 4
   * *
   * 0
   * *
   * 0
   *
   */
  @Test
  def count(): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("streaming-count")
    val batchDuration = Seconds(1)
    val ssc = new StreamingContext(conf, batchDuration)

    val queue = new SynchronizedQueue[RDD[Int]]()
    val qStream = ssc.queueStream(queue)

    val elementCountOfRDD = qStream.count()

    elementCountOfRDD.foreachRDD { rdd =>
      rdd.foreachPartition { iter =>
        println(s"--- ${iter.mkString(",")}")
      }
    }

    qStream.repartition(1).count().foreachRDD { rdd =>
      rdd.foreachPartition { iter =>
        println(s">>> ${iter.mkString(",")}")
      }
    }

    for (i <- 1 until 4) {
      val seq = (0 to i)
      queue += ssc.sparkContext.parallelize(seq)
    }

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 对 stream 每个 rdd 执行计算操作
   * def reduce(reduceFunc: (T, T) => T): DStream[T]
   *
   * --- 1  0+1
   * --- 3 0+1+2
   * --- 6 0+1+2+3
   * ---
   */
  @Test
  def reduce(): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("streaming-reduce")
    val batchDuration = Seconds(1)
    val ssc = new StreamingContext(conf, batchDuration)

    val queue = new SynchronizedQueue[RDD[Int]]()
    val qStream = ssc.queueStream(queue)

    val reducedStream = qStream.reduce(_ + _)

    reducedStream.foreachRDD { rdd =>
      rdd.foreachPartition { iter =>
        println(s"--- ${iter.mkString(",")}")
      }
    }

    for (i <- 1 until 4) {
      val seq = (0 to i)
      queue += ssc.sparkContext.parallelize(seq)
    }

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * tong key 收集到一起
   * def groupByKey(): DStream[(K, Iterable[V])]
   *
   * -------------------------------------------
   * Time: 1582989017000 ms
   * -------------------------------------------
   * (a1,ArrayBuffer(1))
   * (a0,ArrayBuffer(0, 0))
   *
   * -------------------------------------------
   * Time: 1582989018000 ms
   * -------------------------------------------
   * (a1,ArrayBuffer(1, 1))
   * (a0,ArrayBuffer(0, 0, 0))
   * (a2,ArrayBuffer(2))
   *
   * -------------------------------------------
   * Time: 1582989019000 ms
   * -------------------------------------------
   * (a3,ArrayBuffer(3))
   * (a1,ArrayBuffer(1, 1, 1))
   * (a0,ArrayBuffer(0, 0, 0, 0))
   * (a2,ArrayBuffer(2, 2))
   *
   * -----------------------
   */
  @Test
  def groupByKey(): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("streaming-groupByKey")
    val batchDuration = Seconds(1)
    val ssc = new StreamingContext(conf, batchDuration)

    val queue = new SynchronizedQueue[RDD[(String, Int)]]()
    val qStream = ssc.queueStream(queue)

    val groupStream = qStream.groupByKey()

    groupStream.print()

    val dict = Map(0 -> "a0", 1 -> "a1", 2 -> "a2", 3 -> "a3")

    for (i <- 1 until 4) {
      val seq = (0 to i).flatMap(x => (0 to x).map(y => (dict.getOrElse(y, "a0"), y)))
      queue += ssc.sparkContext.parallelize(seq)
    }

    ssc.start()
    ssc.awaitTermination()
  }


  /**
   * 统计kv结构DStream中 value 相同元素个数
   * -------------------------------------------
   * Time: 1582983251000 ms
   * -------------------------------------------
   * ((a0,0),2)  0~1
   * ((a1,1),1)
   *
   * -------------------------------------------
   * Time: 1582983252000 ms
   * -------------------------------------------
   * ((a0,0),3) 0~2
   * ((a2,2),1)
   * ((a1,1),2)
   *
   * -------------------------------------------
   * Time: 1582983253000 ms
   * -------------------------------------------
   * ((a0,0),4) 0~3
   * ((a2,2),2)
   * ((a3,3),1)
   * ((a1,1),3)
   */
  @Test
  def countByValue(): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("streaming-countByValue")
    val batchDuration = Seconds(1)
    val ssc = new StreamingContext(conf, batchDuration)

    val queue = new SynchronizedQueue[RDD[(String, Int)]]()
    val qStream = ssc.queueStream(queue)

    val countByValueStream = qStream.countByValue(2)

    countByValueStream.print()

    val dict = Map(0 -> "a0", 1 -> "a1", 2 -> "a2", 3 -> "a3")

    for (i <- 1 until 4) {
      val seq = (0 to i).flatMap(x => (0 to x).map(y => (dict.getOrElse(y, "a0"), y)))
      queue += ssc.sparkContext.parallelize(seq)
    }

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * kv 结构 Stream 基于key 聚合
   * def reduceByKey(reduceFunc: (V, V) => V): DStream[(K, V)]
   * -------------------------------------------
   * Time: 1582987235000 ms
   * -------------------------------------------
   * (a1,1)
   * (a0,2)
   *
   * -------------------------------------------
   * Time: 1582987236000 ms
   * -------------------------------------------
   * (a1,2)
   * (a0,3)
   * (a2,1)
   *
   * -------------------------------------------
   * Time: 1582987237000 ms
   * -------------------------------------------
   * (a3,1)
   * (a1,3)
   * (a0,4)
   * (a2,2)
   */
  @Test
  def reduceByKey(): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("streaming-countByValue")
    val batchDuration = Seconds(1)
    val ssc = new StreamingContext(conf, batchDuration)

    val queue = new SynchronizedQueue[RDD[(String, Int)]]()
    val qStream = ssc.queueStream(queue)

    val reduceByKeyStream = qStream.map(x => (x._1, 1)).reduceByKey(_ + _)

    reduceByKeyStream.print()

    val dict = Map(0 -> "a0", 1 -> "a1", 2 -> "a2", 3 -> "a3")

    for (i <- 1 until 4) {
      val seq = (0 to i).flatMap(x => (0 to x).map(y => (dict.getOrElse(y, "a0"), y)))
      queue += ssc.sparkContext.parallelize(seq)
    }

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * kv结构流 基于key内连接
   * def join[W: ClassTag](other: DStream[(K, W)]): DStream[(K, (V, W))]
   * -------------------------------------------
   * Time: 1582987812000 ms
   * -------------------------------------------
   * (a1,(10,40))  0~1 1~2
   *
   * -------------------------------------------
   * Time: 1582987813000 ms
   * -------------------------------------------
   * (a1,(20,60)) 0~2 1~3
   * (a2,(20,60))
   */
  @Test
  def join(): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("straming-join")
    val ssc = new StreamingContext(conf, Seconds(1))

    val queue1 = new SynchronizedQueue[RDD[(String, Int)]]()
    val queue2 = new SynchronizedQueue[RDD[(String, Int)]]()

    val qStream1 = ssc.queueStream(queue1)
    val qStream2 = ssc.queueStream(queue2)

    val joinStream = qStream1.join(qStream2)

    joinStream.print()

    for (i <- 0 until 3) {
      queue1 += ssc.sparkContext.parallelize((0 to i)).map((x => (s"a${x}", i * 10)))
    }

    for (i <- 1 until 4) {
      queue2 += ssc.sparkContext.parallelize((1 to i)).map((x => (s"a${x}", i * 20)))
    }

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * kv结构流按key 收集，各流value 自动收集为列表
   * def cogroup[W: ClassTag](other: DStream[(K, W)]): DStream[(K, (Iterable[V], Iterable[W]))]
   * -------------------------------------------
   * Time: 1582988032000 ms
   * -------------------------------------------
   * (a1,(CompactBuffer(),CompactBuffer(20)))
   * (a0,(CompactBuffer(0),CompactBuffer()))
   *
   * -------------------------------------------
   * Time: 1582988033000 ms
   * -------------------------------------------
   * (a1,(CompactBuffer(10),CompactBuffer(40, 40)))
   * (a2,(CompactBuffer(),CompactBuffer(40)))
   * (a0,(CompactBuffer(10, 10),CompactBuffer()))
   *
   * -------------------------------------------
   * Time: 1582988034000 ms
   * -------------------------------------------
   * (a1,(CompactBuffer(20, 20),CompactBuffer(60, 60, 60)))
   * (a2,(CompactBuffer(20),CompactBuffer(60, 60)))
   * (a3,(CompactBuffer(),CompactBuffer(60)))
   * (a0,(CompactBuffer(20, 20, 20),CompactBuffer()))
   */
  @Test
  def cogroup(): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("straming-cogroup")
    val ssc = new StreamingContext(conf, Seconds(1))

    val queue1 = new SynchronizedQueue[RDD[(String, Int)]]()
    val queue2 = new SynchronizedQueue[RDD[(String, Int)]]()

    val qStream1 = ssc.queueStream(queue1)
    val qStream2 = ssc.queueStream(queue2)

    val joinStream = qStream1.cogroup(qStream2)

    joinStream.print()

    for (i <- 0 until 3) {
      queue1 += ssc.sparkContext.parallelize((0 to i)).flatMap(x => (0 to x).map(y => (s"a${y}", i * 10)))
    }

    for (i <- 1 until 4) {
      queue2 += ssc.sparkContext.parallelize((1 to i)).flatMap(x => (1 to x).map(y => (s"a${y}", i * 20)))
    }

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 转换操作，从一种RDD过渡为另一种RDD
   * def transform[U: ClassTag](transformFunc: (RDD[T], Time) => RDD[U]): DStream[U]
   * (f: T => U) 可以使用 case 提取
   * (transformFunc: (RDD[T], Time) => RDD[U]) 不能使用 case 提取
   *
   * def map[U: ClassTag](f: T => U): RDD[U]
   *
   * -------------------------------------------
   * Time: 1582988806000 ms
   * -------------------------------------------
   * (a0,1582988806000 ms)
   *
   * -------------------------------------------
   * Time: 1582988807000 ms
   * -------------------------------------------
   * (a0,1582988807000 ms)
   * (a0,1582988807000 ms)
   * (a1,1582988807000 ms)
   *
   * -------------------------------------------
   * Time: 1582988808000 ms
   * -------------------------------------------
   * (a0,1582988808000 ms)
   * (a0,1582988808000 ms)
   * (a1,1582988808000 ms)
   * (a0,1582988808000 ms)
   * (a1,1582988808000 ms)
   * (a2,1582988808000 ms)
   *
   * -------------------------------------------
   * Time: 1582988809000 ms
   * -------------------------------------------
   */
  @Test
  def transform(): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("straming-transform")
    val ssc = new StreamingContext(conf, Seconds(1))

    val queue1 = new SynchronizedQueue[RDD[(String, Int)]]()
    val qStream1 = ssc.queueStream(queue1)

    val transformStream = qStream1.transform { (rdd, time) =>
      rdd.map { case (k: String, v: Int) =>
        (k, time)
      }
    }

    transformStream.print()

    for (i <- 0 until 3) {
      queue1 += ssc.sparkContext.parallelize((0 to i)).flatMap(x => (0 to x).map(y => (s"a${y}", i * 10)))
    }

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * * 每滑动一次，启动一次计算
   * 窗口流(将若干批次合并为计算窗口，默认滑动补偿为批次，且窗口和滑动步长必须为批次间隔整数倍)
   * (不启用 window 时，默认窗口长度和滑动步长都为 批间隔，每滑动一次，启动一次计算)
   * def window(windowDuration: Duration): DStream[T] = window(windowDuration, this.slideDuration)
   * The window duration of windowed DStream (3000 ms) must be a multiple of the slide duration of parent DStream (2000 ms)
   *
   * -------------------------------------------
   * Time: 1582989864000 ms
   * -------------------------------------------
   * (a0,ArrayBuffer(0))
   *
   * -------------------------------------------
   * Time: 1582989866000 ms
   * -------------------------------------------
   * (a1,ArrayBuffer(10))
   * (a0,ArrayBuffer(0, 10, 10))
   *
   * -------------------------------------------
   * Time: 1582989868000 ms
   * -------------------------------------------
   * (a1,ArrayBuffer(10, 20, 20))
   * (a2,ArrayBuffer(20))
   * (a0,ArrayBuffer(10, 10, 20, 20, 20))
   *
   * -------------------------------------------
   * Time: 1582989870000 ms
   * -------------------------------------------
   * (a1,ArrayBuffer(20, 20, 30, 30, 30))
   * (a2,ArrayBuffer(20, 30, 30))
   * (a3,ArrayBuffer(30))
   * (a0,ArrayBuffer(20, 20, 20, 30, 30, 30, 30))
   *
   * -------------------------------------------
   * Time: 1582989872000 ms
   * -------------------------------------------
   * (a1,ArrayBuffer(30, 30, 30, 40, 40, 40, 40))
   * (a2,ArrayBuffer(30, 30, 40, 40, 40))
   * (a3,ArrayBuffer(30, 40, 40))
   * (a4,ArrayBuffer(40))
   * (a0,ArrayBuffer(30, 30, 30, 30, 40, 40, 40, 40, 40))
   *
   * -------------------------------------------
   * Time: 1582989874000 ms
   * -------------------------------------------
   * (a1,ArrayBuffer(40, 40, 40, 40))
   * (a2,ArrayBuffer(40, 40, 40))
   * (a3,ArrayBuffer(40, 40))
   * (a4,ArrayBuffer(40))
   * (a0,ArrayBuffer(40, 40, 40, 40, 40))
   */
  @Test
  def window(): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("straming-window")
    val batchInterval = Seconds(2)
    val windowDuration = batchInterval * 2

    val ssc = new StreamingContext(conf, batchInterval)

    val queue1 = new SynchronizedQueue[RDD[(String, Int)]]()
    val qStream1 = ssc.queueStream(queue1)

    val windowStream = qStream1.window(windowDuration)
    windowStream.groupByKey().print()

    for (i <- 0 until 5) {
      queue1 += ssc.sparkContext.parallelize((0 to i)).flatMap(x => (0 to x).map(y => (s"a${y}", i * 10)))
      Thread.sleep(1000)
    }

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 每滑动一次，启动一次计算
   * window 创建流默认滑动步长为 批间隔，自定义时必须是批间隔整数倍
   * def window(windowDuration: Duration, slideDuration: Duration): DStream[T]
   *
   * -------------------------------------------
   * Time: 1582990358000 ms
   * -------------------------------------------
   * (a1,ArrayBuffer(10))
   * (a0,ArrayBuffer(0, 10, 10))
   *
   * -------------------------------------------
   * Time: 1582990360000 ms
   * -------------------------------------------
   * (a1,ArrayBuffer(10, 20, 20, 30, 30, 30))
   * (a2,ArrayBuffer(20, 30, 30))
   * (a3,ArrayBuffer(30))
   * (a0,ArrayBuffer(10, 10, 20, 20, 20, 30, 30, 30, 30))
   *
   * -------------------------------------------
   * Time: 1582990362000 ms
   * -------------------------------------------
   * (a1,ArrayBuffer(30, 30, 30, 40, 40, 40, 40))
   * (a2,ArrayBuffer(30, 30, 40, 40, 40))
   * (a3,ArrayBuffer(30, 40, 40))
   * (a4,ArrayBuffer(40))
   * (a0,ArrayBuffer(30, 30, 30, 30, 40, 40, 40, 40, 40))
   *
   */
  @Test
  def windowAndSlide(): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("straming-window")
    val batchInterval = Seconds(1)
    val windowDuration = batchInterval * 3
    val slideDuration = batchInterval * 2

    val ssc = new StreamingContext(conf, batchInterval)

    val queue1 = new SynchronizedQueue[RDD[(String, Int)]]()
    val qStream1 = ssc.queueStream(queue1)

    val windowStream = qStream1.window(windowDuration, slideDuration)
    windowStream.groupByKey().print()

    for (i <- 0 until 5) {
      queue1 += ssc.sparkContext.parallelize((0 to i)).flatMap(x => (0 to x).map(y => (s"a${y}", i * 10)))
      Thread.sleep(1000)
    }

    ssc.start()
    ssc.awaitTermination()
  }

  @Test
  def updateStateByKey(): Unit = {
    val checkpointPath = new File("wordcounts").getAbsolutePath
    val batchDuration: Duration = Seconds(1)
    val checkpointDuration: Duration = batchDuration * 5

    // 状态更新函数 (此处只能声明为 函数，定义为 def 存在序列化问题)
    val updateState = (values: Seq[Int], state: Option[Int]) => {
      val currentIncrement = values.foldLeft(0)(_ + _)
      val prviousValue = state.getOrElse(0)
      Some(prviousValue + currentIncrement)
    }

    // 创建 流 或 从 checkpoint 恢复 流
    val createStreamingContext = () => {
      val conf = new SparkConf().setMaster("local[*]").setAppName("streaming-state")
        .set("spark.streaming.stopGracefullyOnShutdown", "true") // 注册优雅停机钩子
      val ssc = new StreamingContext(conf, batchDuration) // 执行 流切分成批时间间隔
      ssc.checkpoint(checkpointPath)

      val stream = ssc.socketTextStream("localhost", 4000)
      val wordcount = stream.flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_ + _)

      val mergedState = wordcount.updateStateByKey(updateState)
      mergedState.checkpoint(checkpointDuration) // stream 执行 checkpoint 间隔
      mergedState.print()

      ssc
    }

    // 启动流计算
    val ssc = StreamingContext.getOrCreate(checkpointPath, createStreamingContext)

    try {
      ssc.start()
    }
    catch {
      case e: Exception => e.printStackTrace()
    }

    finally {
      ssc.awaitTermination()
    }
  }

  /**
   * stream 输入数据 构成 kv 结构，foreachRDD 中创建 RDD 实现rdd join 操作
   */
  @Test
  def streamJoinOnRdd(): Unit = {
    val checkpointPath = new File("streaming-transform").getAbsolutePath
    val batchDuration: Duration = Seconds(1)
    val checkpointDuration: Duration = batchDuration * 5

    val updateState = (value: Seq[Int], state: Option[Int]) => {
      val currentIncrement = value.foldLeft(0)(_ + _)
      val previousValue = state.getOrElse(0)
      Some(currentIncrement + previousValue)
    }

    val createStreamingContext = () => {
      val conf = new SparkConf().setMaster("local[*]").setAppName("streaming-transform")
        .set("spark.streaming.stopGracefullyOnShutdown", "true") // 注册优雅停机钩子
      val ssc = new StreamingContext(conf, batchDuration)
      ssc.checkpoint(checkpointPath)
      val stream = ssc.socketTextStream("localhost", 4000)
      val wordcount = stream.flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_ + _)
      val mergedState: DStream[(String, Int)] = wordcount.updateStateByKey(updateState)
      mergedState.checkpoint(checkpointDuration)

      mergedState.foreachRDD {
        rdd =>
          val rdd1: RDD[(String, String)] = SparkContext.getOrCreate(conf).parallelize(Seq(("a", "A"), ("b", "B")))
          //  val rdd1: RDD[(String, String)] = ssc.sparkContext.parallelize(Seq(("a", "A"), ("b", "B")))
          // 此 createStreamingContext 函数中 创建rdd 操作必须放在 foreachRDD 中，切不能依靠 ssc 创建
          rdd.join(rdd1).foreach {
            case (key1, (value, key2)) =>
              println(key2, value)
          }
      }
      ssc
    }

    val ssc = StreamingContext.getOrCreate(checkpointPath, createStreamingContext)

    try {
      ssc.start()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      ssc.awaitTermination()
    }
  }

  @Test
  def updateStateByKeyOnwindow(): Unit = {
    val name = "streaming-window"
    val checkpointPath = new File(s"checkpoint/${name}").getAbsolutePath
    val batchDuration: Duration = Seconds(1)
    val checkpointDuration: Duration = batchDuration * 5

    val updateState = (value: Seq[Int], state: Option[Int]) => {
      val currentIncrement = value.foldLeft(0)(_ + _)
      val previousValue = state.getOrElse(0)
      Some(currentIncrement + previousValue)
    }

    val createStreamingContext = () => {
      val conf = new SparkConf().setMaster("local[*]").setAppName(name)
        .set("spark.streaming.stopGracefullyOnShutdown", "true") // 注册优雅停机钩子
      val ssc = new StreamingContext(conf, batchDuration)
      ssc.checkpoint(checkpointPath)
      val stream = ssc.socketTextStream("localhost", 4000)
      val wordcount = stream.flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_ + _)
      val mergedState: DStream[(String, Int)] = wordcount.updateStateByKey(updateState)

      val w3 = mergedState.window(Seconds(3)) // 从当前开始，满3秒开始计算
      val w6 = mergedState.window(Seconds(6)) // 从当前开始，满6秒开始计算

      val joinedStream = w3.join(w6)

      joinedStream.checkpoint(checkpointDuration)
      joinedStream.print()

      ssc
    }

    val ssc = StreamingContext.getOrCreate(checkpointPath, createStreamingContext)

    try {
      ssc.start()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      ssc.awaitTermination()
    }
  }

  /**
   *
   * def reduceByKeyAndWindow(
   * reduceFunc: (V, V) => V,  正运算
   * invReduceFunc: (V, V) => V,  逆运算(从当前状态，可以很方便倒退之前状态) 加快窗口聚合
   * windowDuration: Duration,
   * slideDuration: Duration = self.slideDuration,
   * numPartitions: Int = ssc.sc.defaultParallelism,
   * filterFunc: ((K, V)) => Boolean = null
   * ): DStream[(K, V)]
   */
  @Test
  def reduceByKeyAndWindow(): Unit = {
    val name = "window-slide"
    val batchDuration = Seconds(1)
    val reduceWindow = batchDuration * 3
    val slideWindow = batchDuration * 2
    val checkpointPath = new File(s"checkpoint/${name}").getAbsolutePath

    val updateState = (seq: Seq[Int], state: Option[Int]) => {
      val increment = seq.foldLeft(0)(_ + _)
      val previous = state.getOrElse(0)
      Some(previous + increment)
    }

    // 正运算
    val reduceFunc = (x: Int, y: Int) => x + y
    // 逆运算
    val invReduceFunc = (x: Int, y: Int) => x - y

    val creatingFunc = () => {
      val conf = new SparkConf().setMaster("local[*]").setAppName(name)
      val ssc = new StreamingContext(conf, batchDuration)

      ssc.checkpoint(checkpointPath)

      val stream = ssc.socketTextStream("localhost", 4000)
      val reducedStream = stream.flatMap(_.split("\\s+"))
        .map((_, 1))
        .reduceByKey(_ + _)
        .reduceByKeyAndWindow(reduceFunc, reduceWindow, slideWindow)
      //        .reduceByKeyAndWindow(func, invReduceFunc, reduceWindow, slideWindow)
      // reduceWindow 聚合窗口长度
      // slideWindow 滑动步长

      reducedStream.updateStateByKey(updateState)

      // checkppoint 存在滑动步长时, checkpoint间隔 必须是 slide 的倍数
      // 不存在 slide 时，checkppoint 间隔必须是 batchDuration 整数倍
      reducedStream.checkpoint(slideWindow)
      reducedStream.print()

      ssc
    }

    val ssc = StreamingContext.getOrCreate(checkpointPath, creatingFunc)

    try {
      ssc.start()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      ssc.awaitTermination()
    }
  }

  /**
   * 统计一个窗口中接收元素个数(必须设置 checkpoint 目录)
   * def countByWindow(
   * windowDuration: Duration,
   * slideDuration: Duration): DStream[Long]
   *
   * -------------------------------------------
   * Time: 1582991657000 ms
   * -------------------------------------------
   * 4
   *
   * -------------------------------------------
   * Time: 1582991659000 ms
   * -------------------------------------------
   * 19
   *
   * -------------------------------------------
   * Time: 1582991661000 ms
   * -------------------------------------------
   * 25
   */
  @Test
  def countByWindow(): Unit = {
    val name = "straming-countByWindow"
    val conf = new SparkConf().setMaster("local[*]").setAppName(name)
    val batchInterval = Seconds(1)
    val windowDuration = batchInterval * 3
    val slideDuration = batchInterval * 2

    val checkpointPath = new File(s"checkpoint/${name}").getAbsolutePath
    val ssc = new StreamingContext(conf, batchInterval)
    ssc.checkpoint(checkpointPath)

    val queue1 = new SynchronizedQueue[RDD[(String, Int)]]()
    val qStream1 = ssc.queueStream(queue1)

    val countByValueAndWindowStream = qStream1.countByWindow(windowDuration, slideDuration)
    countByValueAndWindowStream.print()

    for (i <- 0 until 5) {
      queue1 += ssc.sparkContext.parallelize((0 to i)).flatMap(x => (0 to x).map(y => (s"a${y}", i * 10)))
      Thread.sleep(1000)
    }

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 统计window中 value 相同元素个数
   * def countByValueAndWindow(
   * windowDuration: Duration,
   * slideDuration: Duration,
   * numPartitions: Int = ssc.sc.defaultParallelism)
   * (implicit ord: Ordering[T] = null)
   * : DStream[(T, Long)]
   * -------------------------------------------
   * Time: 1582991796000 ms
   * -------------------------------------------
   * ((a0,0),1)
   * ((a1,10),1)
   * ((a0,10),2)
   *
   * -------------------------------------------
   * Time: 1582991798000 ms
   * -------------------------------------------
   * ((a2,20),1)
   * ((a0,30),4)
   * ((a1,10),1)
   * ((a1,20),2)
   * ((a0,20),3)
   * ((a1,30),3)
   * ((a3,30),1)
   * ((a2,30),2)
   * ((a0,10),2)
   *
   * -------------------------------------------
   * Time: 1582991800000 ms
   * -------------------------------------------
   * ((a1,40),4)
   * ((a3,40),2)
   * ((a0,30),4)
   * ((a2,40),3)
   * ((a0,40),5)
   * ((a4,40),1)
   * ((a1,30),3)
   * ((a3,30),1)
   * ((a2,30),2)
   */
  @Test
  def countByValueAndWindow(): Unit = {
    val name = "straming-countByValueAndWindow"
    val conf = new SparkConf().setMaster("local[*]").setAppName(name)
    val batchInterval = Seconds(1)
    val windowDuration = batchInterval * 3
    val slideDuration = batchInterval * 2

    val checkpointPath = new File(s"checkpoint/${name}").getAbsolutePath
    val ssc = new StreamingContext(conf, batchInterval)
    ssc.checkpoint(checkpointPath)

    val queue1 = new SynchronizedQueue[RDD[(String, Int)]]()
    val qStream1 = ssc.queueStream(queue1)

    val countByValueAndWindowStream = qStream1.countByValueAndWindow(windowDuration, slideDuration)
    countByValueAndWindowStream.print()

    for (i <- 0 until 5) {
      queue1 += ssc.sparkContext.parallelize((0 to i)).flatMap(x => (0 to x).map(y => (s"a${y}", i * 10)))
      Thread.sleep(1000)
    }

    ssc.start()
    ssc.awaitTermination()
  }

  @Test
  def remember(): Unit = {
    val name = "straming-remember"
    val conf = new SparkConf().setMaster("local[*]").setAppName(name)
    val batchInterval = Seconds(1)

    val checkpointPath = new File(s"checkpoint/${name}").getAbsolutePath
    val ssc = new StreamingContext(conf, batchInterval)
    ssc.checkpoint(checkpointPath)

    ssc.remember(batchInterval*1)

    val queue1 = new SynchronizedQueue[RDD[String]]()
    val qStream1 = ssc.queueStream(queue1)
    qStream1.map(elem => (elem,1)).reduceByKeyAndWindow((x:Int,y:Int)=> x+y,batchInterval*3,batchInterval*2).print()

    for (i <- 0 until 5) {
      queue1 += ssc.sparkContext.parallelize((0 to i)).flatMap(x => (0 to x).map(y => (s"a${y}")))
      Thread.sleep(1000)
    }

    ssc.start()
    ssc.awaitTermination()
  }

  @Test
  def sqlOnRDD(): Unit = {
    val name = "streaming-sql"
    val checkpointPath = new File(s"checkpoint/${name}").getAbsolutePath
    val batchDuartion = Seconds(1)

    val updateState = (seq: Seq[Int], state: Option[Int]) => {
      val increment = seq.foldLeft(0)(_ + _)
      val previous = state.getOrElse(0)
      Some(previous + increment)
    }

    val createContext = () => {
      val conf = new SparkConf().setMaster("local[*]").setAppName(name)
      val ssc = new StreamingContext(conf, batchDuartion)
      ssc.remember(Minutes(1))

      ssc.checkpoint(checkpointPath)
      val wordcount = ssc.socketTextStream("localhost", 4000)
        .flatMap(_.split("\\s+"))
        .map((_, 1))
        .reduceByKey(_ + _)

      wordcount.updateStateByKey(updateState)
      //      wordcount.checkpoint()
      val windowStream = wordcount.window(batchDuartion * 3, batchDuartion * 2)
      //      windowStream.checkpoint(batchDuartion * 2)

      windowStream.foreachRDD {
        (rdd: RDD[(String, Int)], time: Time) =>
          val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)

          import spark.implicits._

          rdd.toDF("word", "acc").createOrReplaceTempView("wordcount")
          spark.sql("select word,acc from wordcount").show()
      }
      ssc
    }

    val ssc = StreamingContext.getOrCreate(checkpointPath, createContext)

    try {
      ssc.start()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      ssc.awaitTermination()
    }
  }

  @Test
  def test(): Unit ={
    val strings = Set("a", "b")
    println(strings("a")) // 检查 字符串是否存在于 Set
    println(strings("c"))

  }





}
