package com.bigdata.demo.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, RddSuit, SparkConf, SparkContext}
import org.junit.Test

class RddInfo {

  @Test
  def createRdd(): Unit ={
    val conf = new SparkConf().setAppName("rdd").setMaster("local[*]")
    lazy val sc = new SparkContext(conf)
    try {
      // 不指定分区，默认与 setMaster 一致使用核数一致 local[*] 默认为最大物理核数
      println("---- makeRDD -----")
      sc.makeRDD(0 to 16).foreachPartition(x => println(x.mkString(",")))
      /*
        12,13
        14,15,16
        0,1
        8,9
        10,11
        4,5
        6,7
        2,3
       */

      // 指定分区，为分区数
      println("---- makeRDD 4-----")
      sc.makeRDD(0 to 16,4).foreachPartition(x=> println(x.mkString(",")))
      /*
        8,9,10,11
        4,5,6,7
        12,13,14,15,16
        0,1,2,3
       */

      println("---- parallelize -----")
      sc.parallelize(0 to 16).foreachPartition(x=> println(x.mkString(",")))
      /*
        12,13
        10,11
        0,1
        4,5
        14,15,16
        2,3
        6,7
        8,9
       */

      println("---- parallelize 10 -----")
      sc.parallelize(0 to 16,10).foreachPartition(x=> println(x.mkString(",")))
      /* 超过物理核数，为微核数
        1,2
        8,9
        11,12
        6,7
        10
        0
        3,4
        5
        13,14
        15,16
       */

      println("---- 从我外部系统读取文件 ----")
      val rdd = sc.textFile("hdfs://hadoop01:9000/apps/mr/wc/in/*")
      val rdd2 = rdd.flatMap(_.split("\\s+"))

      rdd2.map((_,1))
        .reduceByKey(_+_)
        .foreachPartition(x=>println(x.mkString(",")))

      /*
       * (f,2),(cc,2),(c,2)
       * (aa,2),(e,2),(ss,1),(b,2)
       * (d,4),(s,2),(a,2),(bb,2)
       */


    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      sc.stop()
    }
  }

  /**
   * RDD 调用 cache 或 persist 方法，可以将前面计算结果一序列化形式存储在 JVM 堆空间，供后面计算节点复用
   * 1.rdd约定的是计算逻辑，每次执行 action 操作都会重新计算，被缓存的 rdd ，约定的是缓存时刻的值，后面直接复用
   * 2.cache 需要使用 action 算子触发，否则不会生效
   * 3.cache 不会截断依赖链，被 cache 的rdd节点发生故障，可以依据依赖连，重新构建 rdd
   * 4.cache 底层调用的时 persist(StorageLevel.MemoryOnly)策略
   * 5.cache主要是为提升重复逻辑的计算效率而存在，节点宕机 cache 失效，而 checkpoint 主要是为容错而存在，
   * 且 执行 checkpoint 前都需要执行 cache，否则会重新提交之前任务，checkpoint斩断了依赖链，不能基于依赖链重新恢复 rdd,但可从文件恢复 rdd
   *
   * cacheRdd init: deps:1, data:1580518538675,1580518538675,1580518538676
   * from cacheRdd: deps:1, data:1580518538675,1580518538675,1580518538676
   * unCachedRdd: deps:1, data:1580518538804,1580518538802,1580518538803
   * from unCachedRdd: deps:1, data:1580518538827,1580518538824,1580518538827
   *
   */
  @Test
  def cache(): Unit ={
    val conf = new SparkConf().setAppName("cache").setMaster("local[*]")
    val sc = new SparkContext(conf)
    try {
      val cachedRdd = sc.parallelize(0 to 2).map(_=>System.currentTimeMillis())
      cachedRdd.cache()
      println(s"cacheRdd-deps: ${cachedRdd.dependencies.size}")

      val res1 = cachedRdd.collect().mkString(",")
      println(s"cacheRdd init: data:${res1}")

      val res2 = cachedRdd.collect().mkString(",")
      println(s"from cacheRdd: data:${res2}")

      val unCachedRdd = sc.parallelize(0 to 2).map(_=>System.currentTimeMillis())
      println(s"cacheRdd-deps: ${unCachedRdd.dependencies.size}")

      val res3 = unCachedRdd.collect().mkString(",")
      println(s"unCachedRdd: data:${res3}")

      val res4= unCachedRdd.collect().mkString(",")
      println(s"from unCachedRdd: data:${res4}")

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      sc.stop()
    }
  }

  @Test
  def persist(): Unit ={
    val conf = new SparkConf().setAppName("persist").setMaster("local[*]")
    val sc = new SparkContext(conf)
    try {
      val cachedRdd = sc.parallelize(0 to 2).map(_=>System.currentTimeMillis())
      cachedRdd.persist(StorageLevel.OFF_HEAP)
      println(s"cacheRdd-deps: ${cachedRdd.dependencies.size}")

      val res1 = cachedRdd.collect().mkString(",")
      println(s"cacheRdd init: data:${res1}")

      val res2 = cachedRdd.collect().mkString(",")
      println(s"from cacheRdd: data:${res2}")

      val unCachedRdd = sc.parallelize(0 to 2).map(_=>System.currentTimeMillis())
      println(s"cacheRdd-deps: ${unCachedRdd.dependencies.size}")

      val res3 = unCachedRdd.collect().mkString(",")
      println(s"unCachedRdd: data:${res3}")

      val res4= unCachedRdd.collect().mkString(",")
      println(s"from unCachedRdd: data:${res4}")
      /**
       * val NONE = new StorageLevel(false, false, false, false) 不执行缓存
       * val DISK_ONLY = new StorageLevel(true, false, false, false) 只缓存在磁盘
       * val DISK_ONLY_2 = new StorageLevel(true, false, false, false, 2) 缓存磁盘两份
       * val MEMORY_ONLY = new StorageLevel(false, true, false, true) 只缓存在内存
       * val MEMORY_ONLY_2 = new StorageLevel(false, true, false, true, 2)
       * val MEMORY_ONLY_SER = new StorageLevel(false, true, false, false) 序列化形式缓存在内存
       * val MEMORY_ONLY_SER_2 = new StorageLevel(false, true, false, false, 2)
       * val MEMORY_AND_DISK = new StorageLevel(true, true, false, true) 先缓存在内存，存不下写入磁盘
       * val MEMORY_AND_DISK_2 = new StorageLevel(true, true, false, true, 2)
       * val MEMORY_AND_DISK_SER = new StorageLevel(true, true, false, false)
       * val MEMORY_AND_DISK_SER_2 = new StorageLevel(true, true, false, false, 2)
       * val OFF_HEAP = new StorageLevel(true, true, true, false, 1) // 非堆内存（使用 Tachyon 实现对外缓存）
       */
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      sc.stop()
    }
  }

  /**
   * RDD 执行checkpoint，将数据序列化存储到磁盘指定目录
   * 1.checkpoint 前需要执行 cache 操作， 否则或重复将目标 rdd 重新提交执行，并且通 cache 一样，也需要执行算子触发
   * 2.checkpoint 之前需要实现设置 checkpointDir,从文件恢复过程需要指定到 part-xxxx 父目录；
   * 3.从 checkpointFile恢复 rdd 需要声明类型
   * 4.从 checkpointFile 中恢复的 rdd为 ReliableCheckpointRDD,且依赖链被斩断
   * checkpoint-deps: 1, data: (0,1580526236017),(1,1580526236019),(2,1580526236018)
   * checkpoint-deps: 0, data: (0,1580526236017),(1,1580526236019),(2,1580526236018)
   * checkpoint-deps: 0, data: (0,1580526236017),(1,1580526236019),(2,1580526236018)
   *
   */
  @Test
  def checkpoint(): Unit ={
    val conf = new SparkConf().setAppName("cache").setMaster("local[*]")
    implicit val sc = new SparkContext(conf)
    try {
      val checkpointDir = "spark-core/rdd-info/checkpoint"
//      val checkpointDir = "hdfs://hadoop01:9000/user/spark/checkpoint"
      sc.setCheckpointDir(checkpointDir)
      val toCheckpointRdd = sc.parallelize(0 to 2).map((_,System.currentTimeMillis()))
      println(s"checkpointed: ${toCheckpointRdd.isCheckpointed}") // false
      toCheckpointRdd.cache()
      println(s"checkpointed: ${toCheckpointRdd.isCheckpointed}") // false
      toCheckpointRdd.checkpoint()
      println(s"checkpointed: ${toCheckpointRdd.isCheckpointed}") // false

      println(s"checkpoint-deps: ${toCheckpointRdd.dependencies.size}, data: ${toCheckpointRdd.collect().mkString(",")}")

      println(s"checkpointed: ${toCheckpointRdd.isCheckpointed}") // true 执行算子之后被标记为 true

      val checkpointFile = toCheckpointRdd.getCheckpointFile.get

      val fromCheckpointRdd1:RDD[(Int,Long)] = RddSuit.getCheckpointRdd(sc,checkpointFile)
      println(s"checkpoint-deps: ${fromCheckpointRdd1.dependencies.size}, data: ${fromCheckpointRdd1.collect().mkString(",")}")

      val fromCheckpointRdd2:RDD[(Int,Long)] = RddSuit.getCheckpointRdd2(checkpointFile)
      println(s"checkpoint-deps: ${fromCheckpointRdd2.dependencies.size}, data: ${fromCheckpointRdd2.collect().mkString(",")}")

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      sc.stop()
    }
  }

  @Test
  def dependency(): Unit ={
    val conf = new SparkConf().setAppName("deps").setMaster("local[*]")
    val sc = new SparkContext(conf)
    try {
      val rdd1 = sc.parallelize(0 to 3)

      val rdd2 = sc.makeRDD(0 to 3)

      val rdd3 = sc.textFile("hdfs://hadoop01:9000/apps/mr/wc/in")
      val rdd4 = rdd3.flatMap(_.split("\\s+"))
      val rdd5 = rdd4.map((_,1))
      val rdd6 = rdd5.reduceByKey(_+_)

      val rdd7 = rdd5.reduceByKey(_+_)
      val rdd8 = rdd6.join(rdd7)

      println(s"""
           |size1: ${rdd1.dependencies.size}, data:${rdd1.dependencies.mkString(",")}
           |size2: ${rdd2.dependencies.size}, data:${rdd2.dependencies.mkString(",")}
           |size3: ${rdd3.dependencies.size}, data:${rdd3.dependencies.mkString(",")}
           |size4: ${rdd4.dependencies.size}, data:${rdd4.dependencies.mkString(",")}
           |size5: ${rdd5.dependencies.size}, data:${rdd5.dependencies.mkString(",")}
           |size6: ${rdd6.dependencies.size}, data:${rdd6.dependencies.mkString(",")}
           |size7: ${rdd7.dependencies.size}, data:${rdd7.dependencies.mkString(",")}
           |size8: ${rdd8.dependencies.size}, data:${rdd8.dependencies.mkString(",")}
           |""".stripMargin)

      /**
       * size1: 0, data:
       * size2: 0, data:
       * size3: 1, data:org.apache.spark.OneToOneDependency@670f2466  窄依赖（来源于唯一父RDD,父与子时一对一关系）
       * size4: 1, data:org.apache.spark.OneToOneDependency@6e03db1f
       * size5: 1, data:org.apache.spark.OneToOneDependency@bfec2f9
       * size6: 1, data:org.apache.spark.ShuffleDependency@6cbe68e9 宽依赖(来源于多个父 RDD)，发生了网络混洗
       * size7: 1, data:org.apache.spark.ShuffleDependency@750a04ec
       * size8: 1, data:org.apache.spark.OneToOneDependency@49e2b3c5
       */

      rdd6.foreachPartition(x=>println(x.mkString(",")))

      /**
       * (f,2),(cc,2),(c,2)
       * (d,4),(s,2),(a,2),(bb,2)
       * (aa,2),(e,2),(ss,1),(b,2)
       */

      rdd6.dependencies.foreach{x=>
        x.rdd.foreachPartition(x=>println(x.mkString(",")))
//        x.rdd.dependencies 一层层往上溯源
      }

      /**
       * (a,1),(b,1),(c,1),(d,1),(d,1),(e,1),(f,1),(s,1)
       * (a,1),(b,1),(c,1),(d,1),(d,1),(e,1),(f,1),(s,1)
       * (aa,1),(bb,1),(cc,1),(bb,1),(ss,1),(aa,1),(cc,1)
       */

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      sc.stop()
    }
  }

  /**
   * 初始化创建 RDD，不存在分区器
   */
  @Test
  def partitioner(): Unit ={
    val conf = new SparkConf().setAppName("partitioner").setMaster("local[*]")
    val sc = new SparkContext(conf)
    try {
      val rdd = sc.parallelize(Seq(('A',1),('B',1),('C',1)))
      println(rdd.partitioner) // None

      val rdd2 = rdd.partitionBy(new HashPartitioner(3))
      println(rdd2.partitioner) // Some(org.apache.spark.HashPartitioner@3)
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      sc.stop()
    }
  }















}
