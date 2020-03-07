package com.bigdata.demo.spark.streaming

import java.io.{File, Serializable}

import breeze.linalg.min
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import com.bigdata.demo.spark.util.{KafkaProducerProxy, createKafkaProducerPool}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.{Err, LeaderOffset}
import org.junit.Test
import com.bigdata.demo.spark.util.SetupJdbc

import scala.collection.mutable
import com.typesafe.config.ConfigFactory
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import scalikejdbc._


class CreateStreamV0_11_0_0 {

  val logger = Logger.getLogger(getClass.getSimpleName)

  /**
   * 1.连接 zk,获取 group 对指定 topic 消费的元数据信息
   * 成功获取 -> 表明之前存在消费关系，
   * 在分区数不变情况下，需要继续获取当前 topic 可消费offset 信息
   * 如果可消费的offset < 上次记录到的offset, 表明 topic 被删除重建了，直接从最早开消费，否则继续冲上次消费位置开始消费
   * 如果分区数发生改变，调整分区了，需要手动修改kafka 上group-offset 信息,然后定位读取
   * 获取失败 -> 表明是初次消费，直接从最早开始消费
   */
  @Test
  def saveOffsetToZk() {
    val conf = new SparkConf().setMaster("local[4]").setAppName("NetworkWordCount")
      .set("spark.worker.timeout", "500")
      .set("spark.cores.max", "10") // 最大核数
      .set("spark.streaming.kafka.maxRatePerPartition", "100") // 每个partition 一次处理消息数
      .set("spark.rpc.askTimeout", "600s") // spark 访问 kafka rpc 超时
      .set("spark.network.timeout", "600s")
      .set("spark.streaming.backpressure.enabled", "true") // kafka启用背压机制
      .set("spark.task.maxFailures", "1") // 失败重试次数
      .set("spark.speculationfalse", "false") // 关闭推断优化执行，保持幂等性
      .set("spark.streaming.stopGracefullyOnShutdown", "true") // 优雅停机
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")

    val ssc = new StreamingContext(conf, Seconds(1))

    val checkpointPath = new File("save_offset_to_zk").getAbsolutePath
    ssc.checkpoint(checkpointPath)

    // 创建topic
    val brokers = "hadoop01:9092,hadoop02:9092,hadoop03:9092"
    val sourceTopic = "test1"
    val topics = Map(sourceTopic -> 1)

    val sinkTopic = "test2"

    val kafkaConf = Map(
      "metadata.broker.list" -> "hadoop01:9092,hadoop02:9092,hadoop03:9092",
      "zookeeper.connect" -> "hadoop01:2181,hadoop02:2181,hadoop03:2181",
      "group.id" -> "streaming_v25",
      "zookeeper.connection.timeout.ms" -> "1000",
      "auto.offset.reset" -> "smallest"
    )

    val stream = KafkaUtils.createStream[Array[Byte], String, DefaultDecoder, StringDecoder](ssc, kafkaConf,topics,StorageLevel.MEMORY_AND_DISK_2)

    // 广播kafka连接池
    val pool: GenericObjectPool[KafkaProducerProxy] = createKafkaProducerPool(brokers, sinkTopic)
    val broadcastPool: Broadcast[GenericObjectPool[KafkaProducerProxy]] = ssc.sparkContext.broadcast(pool)

    stream.foreachRDD { rdd =>
      rdd.foreachPartition { iter =>
        val pool = broadcastPool.value
        val p = pool.borrowObject()
        iter.foreach { msg =>
          val key = Bytes.toInt(msg._1)
          val value = msg._2
          p.send(value, Option(sinkTopic))
          println(s"key: ${key}, value: ${value}")
        }
        pool.returnObject(p)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }


  /**
   * 1.连接 zk,获取 group 对指定 topic 消费的元数据信息
   * 成功获取 -> 表明之前存在消费关系，
   * 在分区数不变情况下，需要继续获取当前 topic 可消费offset 信息
   * 如果可消费的offset < 上次记录到的offset, 表明 topic 被删除重建了，直接从最早开消费，否则继续冲上次消费位置开始消费
   * 如果分区数发生改变，调整分区了，需要手动修改kafka 上group-offset 信息,然后定位读取
   * 获取失败 -> 表明是初次消费，直接从最早开始消费
   */
  @Test
  def saveOffsetToDB() {
    val conf = new SparkConf().setMaster("local[4]").setAppName("NetworkWordCount")
      .set("spark.worker.timeout", "500")
      .set("spark.cores.max", "10") // 最大核数
      .set("spark.streaming.kafka.maxRatePerPartition", "100") // 每个partition 一次处理消息数
      .set("spark.rpc.askTimeout", "600s") // spark 访问 kafka rpc 超时
      .set("spark.network.timeout", "600s")
      .set("spark.streaming.backpressure.enabled", "true") // kafka启用背压机制
      .set("spark.task.maxFailures", "1") // 失败重试次数
      .set("spark.speculationfalse", "false") // 关闭推断优化执行，保持幂等性
      .set("spark.streaming.stopGracefullyOnShutdown", "true") // 优雅停机
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")

    val ssc = new StreamingContext(conf, Seconds(1))

    val checkpointPath = new File("save_offset_to_db").getAbsolutePath
    ssc.checkpoint(checkpointPath)

    // 创建topic
    val brokers = "hadoop01:9092,hadoop02:9092,hadoop03:9092"
    val sourceTopic = "test1"
    val topics = Map(sourceTopic -> 1)
    val sinkTopic = "test2"

    //创建消费者组
    val group = "streaming_v1"

    // 消费者配置 Valid values are smallest and largest
    val kafkaParam = Map(
      "bootstrap.servers" -> brokers,
      "group.id" -> group,
      "auto.offset.reset" -> "smallest",
      "enable.auto.commit" -> "false"
    )

    // 从 db 查找group topic offset 信息
    val stream: InputDStream[(Array[Byte], String)] = KafkaUtils.createStream[Array[Byte], String, DefaultDecoder, StringDecoder](ssc, kafkaParam, topics, StorageLevel.MEMORY_AND_DISK_2)

    // 广播kafka连接池
    val pool: GenericObjectPool[KafkaProducerProxy] = createKafkaProducerPool(brokers, sinkTopic)
    val broadcastPool: Broadcast[GenericObjectPool[KafkaProducerProxy]] = ssc.sparkContext.broadcast(pool)

    stream.foreachRDD { rdd =>
      rdd.foreachPartition { iter =>
        val pool = broadcastPool.value
        val p = pool.borrowObject()
        iter.foreach { msg =>
          val key = Bytes.toInt(msg._1)
          val value = msg._2
          p.send(value, Option(sinkTopic))
          println(msg)
        }
        pool.returnObject(p)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
