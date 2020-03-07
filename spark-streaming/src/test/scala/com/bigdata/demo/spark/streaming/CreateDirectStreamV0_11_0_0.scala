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
import scalikejdbc.DB
import com.bigdata.demo.spark.util.SetupJdbc
import scala.collection.mutable
import com.typesafe.config.ConfigFactory
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import scalikejdbc._



class CreateDirectStreamV0_11_0_0 {

  val logger = Logger.getLogger(getClass.getSimpleName)

  /**
   * 1.连接 zk,获取 group 对指定 topic 消费的元数据信息
   *  成功获取 -> 表明之前存在消费关系，
   *    在分区数不变情况下，需要继续获取当前 topic 可消费offset 信息
   *      如果可消费的offset < 上次记录到的offset, 表明 topic 被删除重建了，直接从最早开消费，否则继续冲上次消费位置开始消费
   *    如果分区数发生改变，调整分区了，需要手动修改kafka 上group-offset 信息,然后定位读取
   *  获取失败 -> 表明是初次消费，直接从最早开始消费
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
      .set("spark.streaming.stopGracefullyOnShutdown","true") // 优雅停机

    val ssc = new StreamingContext(conf, Seconds(1))

    val checkpointPath = new File("save_offset_to_zk").getAbsolutePath
    ssc.checkpoint(checkpointPath)

    // 创建topic
    val brokers = "hadoop01:9092,hadoop02:9092,hadoop03:9092"
    val sourceTopic = "test1"
    val sinkTopic = "test2"

    //创建消费者组
    val group= "streaming_v20"

    // 消费者配置 Valid values are smallest and largest
    val kafkaParam = Map(
      "bootstrap.servers" -> brokers,
      "group.id" -> group,
      "auto.offset.reset" -> "smallest",
      "enable.auto.commit" -> "false"
    )

    var stream:InputDStream[(Array[Byte], String)] = null
    val cluster = new KafkaCluster(kafkaParam)
    val topicAndPartitions:Set[TopicAndPartition] = cluster.getPartitions(Set(sourceTopic)).right.get
    val recordEither = cluster.getConsumerOffsets(group, topicAndPartitions)

    if(recordEither.isRight){
      val recordMeta:Map[TopicAndPartition, Long] = recordEither.right.get
      logger.info(s"old consumer group: ${group}\nrecored-offset: ${recordMeta.mkString(",")}")
      // zk有offset记录
      val currentMeta:Map[TopicAndPartition, Long] = cluster.getLatestLeaderOffsets(topicAndPartitions).right.get.map(x=>(x._1,x._2.offset))

      // 分区数不变情况，下面策略是有效的，分区数变化，可能需要手动修改 zk 上记录的偏移量信息了
      val deleteAndCreated = currentMeta.keySet.count{topicAndPartition=>
        // 只要出现某分区当前最大可消费的offset < 上次记录此分区已经消费了的offset，则表明topic 是删除之后重建的，需要从最早可消费处开始消费
        currentMeta.get(topicAndPartition).get < recordMeta.get(topicAndPartition).get
      } > 0

      logger.info(s"current producer offset: ${currentMeta.mkString(",")}")
      if(deleteAndCreated){ // 删除重建的
        logger.info("topic has been created after delete, consume from beginning")
        stream = KafkaUtils.createDirectStream[Array[Byte], String, DefaultDecoder, StringDecoder](ssc, kafkaParam,Set(sourceTopic))
      }else{// 接着前面的消费
        logger.info(s"group continue consume")
        val messageHandler = (mam: MessageAndMetadata[Array[Byte], String]) => (mam.key(),mam.message())
        stream = KafkaUtils.createDirectStream[Array[Byte], String, DefaultDecoder, StringDecoder,(Array[Byte], String)](
          ssc, kafkaParam, recordMeta,messageHandler)
      }
    }else{
      // zk无offset记录
      logger.info(s"new consumer group: ${group}")
      stream = KafkaUtils.createDirectStream[Array[Byte], String, DefaultDecoder, StringDecoder](ssc, kafkaParam,Set(sourceTopic))
    }

    // 广播kafka连接池
    val pool:GenericObjectPool[KafkaProducerProxy] = createKafkaProducerPool(brokers, sinkTopic)
    val broadcastPool: Broadcast[GenericObjectPool[KafkaProducerProxy]] = ssc.sparkContext.broadcast(pool)

    stream.foreachRDD{ rdd =>
      val offsetRanges:Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val unconsumed = mutable.HashMap[Int,(Map[TopicAndPartition,Long],OffsetRange)]()
      offsetRanges.filter(offsets=> offsets.fromOffset<offsets.untilOffset).foreach{offsets=>
        val topicAndPartition = TopicAndPartition(sourceTopic, offsets.partition)
        unconsumed.put(offsets.partition,(Map((topicAndPartition, offsets.untilOffset)),offsets))
      }

      val cluster = new KafkaCluster(kafkaParam)

      // test1 消息消费到 test2
      rdd.mapPartitionsWithIndex{ (partition,iter) =>
        if(unconsumed.contains(partition)){
          // 消费
//          val pool = createKafkaProducerPool(brokers, sinkTopic)
          val pool = broadcastPool.value
          val p = pool.borrowObject()
          iter.foreach{ msg =>
            val key = Bytes.toInt(msg._1)
            val value = msg._2
            p.send(value,Option(sinkTopic))
          }
          pool.returnObject(p)

          // 提交 offset
          val meta:(Map[TopicAndPartition,Long],OffsetRange) = unconsumed.get(partition).get
          val either:Either[Err, Map[TopicAndPartition, Short]] = cluster.setConsumerOffsets(group,  meta._1)
          if (either.isLeft) {
            println(s"Error updating the offset to Kafka cluster: ${either.left.get}")
          }else{
            val offsetRange:OffsetRange = meta._2
            println(offsetRange.toString())
//            if(offsetRange.untilOffset < 100 ){
//              throw new RuntimeException("test")
//            }
          }
        }
        Iterator("")
      }.collect()
    }

    ssc.start()
    ssc.awaitTermination()
  }


  /**
   * 1.连接 zk,获取 group 对指定 topic 消费的元数据信息
   *  成功获取 -> 表明之前存在消费关系，
   *    在分区数不变情况下，需要继续获取当前 topic 可消费offset 信息
   *      如果可消费的offset < 上次记录到的offset, 表明 topic 被删除重建了，直接从最早开消费，否则继续冲上次消费位置开始消费
   *    如果分区数发生改变，调整分区了，需要手动修改kafka 上group-offset 信息,然后定位读取
   *  获取失败 -> 表明是初次消费，直接从最早开始消费
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
      .set("spark.streaming.stopGracefullyOnShutdown","true") // 优雅停机

    val ssc = new StreamingContext(conf, Seconds(1))

    val checkpointPath = new File("save_offset_to_db").getAbsolutePath
    ssc.checkpoint(checkpointPath)

    // 创建topic
    val brokers = "hadoop01:9092,hadoop02:9092,hadoop03:9092"
    val sourceTopic = "test1"
    val sinkTopic = "test2"

    //创建消费者组
    val group= "streaming_v1"

    // 消费者配置 Valid values are smallest and largest
    val kafkaParam = Map(
      "bootstrap.servers" -> brokers,
      "group.id" -> group,
      "auto.offset.reset" -> "smallest",
      "enable.auto.commit" -> "false"
    )


    // 从 db 查找group topic offset 信息
    SetupJdbc()
    val recordMeta = DB.readOnly{ implicit session =>
      sql"select topic, `partition`, offset from streaming_offset where group_id=$group"
        .map{resultSet =>
          // TopicAndPartition -> offset
          if(null!=resultSet){
            TopicAndPartition(resultSet.string(1), resultSet.int(2)) -> resultSet.long(3)
          }else{
            null
          }
        }.list.apply().toMap
    }

    var stream:InputDStream[(Array[Byte], String)] = null
    val cluster = new KafkaCluster(kafkaParam)
    val topicAndPartitions:Set[TopicAndPartition] = cluster.getPartitions(Set(sourceTopic)).right.get

    if(recordMeta.size>0){
      // group-topic 有过消费记录
      logger.info(s"old consumer group: ${group}\nrecored-offset: ${recordMeta.mkString(",")}")
      val currentMeta:Map[TopicAndPartition, Long] = cluster.getLatestLeaderOffsets(topicAndPartitions).right.get.map(x=>(x._1,x._2.offset))
      // 分区数不变情况下，各分区可消费的offset都 >= 上次记录的，说明 topic 很可能仍旧有效，直接从上次位置开始继续消费，否则topic 可能是删除之后重建的，需要冲最早开消费
      // 如果分区数变化了，需要调整db存储的 group-topic-partition-offset信息，才能继续接着消费
      val deleteAndCreated = currentMeta.keySet.count{topicAndPartition=>
        // 只要出现某分区当前最大可消费的offset < 上次记录此分区已经消费了的offset，则表明topic 是删除之后重建的，需要从最早可消费处开始消费
        currentMeta.get(topicAndPartition).get < recordMeta.get(topicAndPartition).get
      } > 0

      logger.info(s"current producer offset: ${currentMeta.mkString(",")}")
      if(deleteAndCreated){ // 删除重建的
        logger.info("topic has been created after delete, consume from beginning")
        stream = KafkaUtils.createDirectStream[Array[Byte], String, DefaultDecoder, StringDecoder](ssc, kafkaParam,Set(sourceTopic))
      }else{
        // 接着前面的消费
        logger.info(s"group continue consume")
        val messageHandler = (mam: MessageAndMetadata[Array[Byte], String]) => (mam.key(),mam.message())
        stream = KafkaUtils.createDirectStream[Array[Byte], String, DefaultDecoder, StringDecoder,(Array[Byte], String)](
          ssc, kafkaParam, recordMeta,messageHandler)
      }
    }else{
      // 新 group - topic 直接从最早开始消费
      logger.info(s"new consumer group: ${group}")
      stream = KafkaUtils.createDirectStream[Array[Byte], String, DefaultDecoder, StringDecoder](ssc, kafkaParam,Set(sourceTopic))
    }

    // 广播kafka连接池
    val pool:GenericObjectPool[KafkaProducerProxy] = createKafkaProducerPool(brokers, sinkTopic)
    val broadcastPool: Broadcast[GenericObjectPool[KafkaProducerProxy]] = ssc.sparkContext.broadcast(pool)

    stream.foreachRDD{ rdd =>
      val offsetRanges:Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val unconsumed = mutable.HashMap[Int,(Map[TopicAndPartition,Long],OffsetRange)]()
      offsetRanges.filter(offsets=> offsets.fromOffset<offsets.untilOffset).foreach{offsets=>
        val topicAndPartition = TopicAndPartition(sourceTopic, offsets.partition)
        unconsumed.put(offsets.partition,(Map((topicAndPartition, offsets.untilOffset)),offsets))
      }

      // test1 消息消费到 test2
      rdd.mapPartitionsWithIndex{ (partition,iter) =>
        if(unconsumed.contains(partition)){
          // 若没有广播kafka 连接池，则为需要才partition级别转换操作中创建(至少在partition 中是共用的)
          // val pool = createKafkaProducerPool(brokers, sinkTopic)
          val pool = broadcastPool.value
          val p = pool.borrowObject()
          iter.foreach{ msg =>
            val key = Bytes.toInt(msg._1)
            val value = msg._2
            p.send(value,Option(sinkTopic))
          }
          pool.returnObject(p)

          // 提交 offset
          val meta:(Map[TopicAndPartition,Long],OffsetRange) = unconsumed.get(partition).get
          DB.localTx { implicit session =>
            unconsumed.values.foreach{x =>
              val offsetrange = x._2
              try {
                sql"replace into streaming_offset values(${offsetrange.topic}, ${offsetrange.partition}, ${offsetrange.untilOffset}, ${group})".update.apply()
                println(offsetrange.toString())
              } catch {
                case e:Exception => e.printStackTrace()
              }
            }
          }
        }
        Iterator("")
      }.collect()
    }

    ssc.start()
    ssc.awaitTermination()
  }


}
