package com.bigdata.demo.spark.streaming

import breeze.linalg.max
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.common.TopicAndPartition
import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.kafka.{KafkaUtils,HasOffsetRanges, OffsetRange,KafkaCluster}

import com.typesafe.config.ConfigFactory
import scalikejdbc._

object SetupJdbc1 {
  def apply(driver: String, host: String, user: String, password: String): Unit = {
    Class.forName(driver)
    ConnectionPool.singleton(host, user, password)
  }
}

object SimpleApp{
  def main(args: Array[String]): Unit = {
    // 加载工程resources目录下application.conf文件，该文件中配置了databases信息，以及topic及group消息
    val conf = ConfigFactory.load("spark-streaming.conf")
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> conf.getString("kafka.brokers"),
      "group.id" -> conf.getString("kafka.group"),
      "auto.offset.reset" -> "smallest" // 重置为最小 offset
    )
    val jdbcDriver = conf.getString("jdbc.driver")
    val jdbcUrl = conf.getString("jdbc.url")
    val jdbcUser = conf.getString("jdbc.user")
    val jdbcPassword = conf.getString("jdbc.password")

    val topic = conf.getString("kafka.topics")
    val group = conf.getString("kafka.group")

    val ssc = setupSsc(kafkaParams, jdbcDriver, jdbcUrl, jdbcUser, jdbcPassword,topic, group)()
    ssc.start()
    ssc.awaitTermination()
  }

  def createStream(taskOffsetInfo: Map[TopicAndPartition, Long], kafkaParams: Map[String, String], conf:SparkConf, ssc: StreamingContext, topics:String):InputDStream[_] = {
    // 若taskOffsetInfo 不为空， 说明这不是第一次启动该任务, database已经保存了该topic下该group的已消费的offset, 则对比kafka中该topic有效的offset的最小值和数据库保存的offset，取比较大作为新的offset.
    if(taskOffsetInfo.size != 0){
      val kc = new KafkaCluster(kafkaParams)
      val earliestLeaderOffsets = kc.getEarliestLeaderOffsets(taskOffsetInfo.keySet)
      if(earliestLeaderOffsets.isLeft)
        throw new SparkException("get kafka partition failed:")
      val earliestOffSets = earliestLeaderOffsets.right.get

      val offsets = earliestOffSets.map(r =>
        TopicAndPartition(r._1.topic, r._1.partition) -> r._2.offset.toLong)

      val newOffsets = taskOffsetInfo.map(r => {
          val t = offsets(r._1) // TopicAndPartition
          r._1 -> max(t,r._2)
        }
      )
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => 1L
      // kafka 定位读取
      //val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, Long](ssc, kafkaParams, newOffsets, messageHandler)
    } else {
      val topicSet = topics.split(",").toSet
      // 首次启动从最新开始读取
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams,topicSet)
    }
  }

  def setupSsc(
                kafkaParams: Map[String, String],
                jdbcDriver: String,
                jdbcUrl: String,
                jdbcUser: String,
                jdbcPassword: String,
                topics:String,
                group:String
              )(): StreamingContext = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("offset")
      .set("spark.worker.timeout", "500")
      .set("spark.cores.max", "10")
      .set("spark.streaming.kafka.maxRatePerPartition", "500")
      .set("spark.rpc.askTimeout", "600s")
      .set("spark.network.timeout", "600s")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.task.maxFailures", "1")
      .set("spark.speculationfalse", "false") // 关闭推断优化执行，保持幂等性
      .set("spark.streaming.stopGracefullyOnShutdown","true")

    val ssc = new StreamingContext(conf, Seconds(5))
    SetupJdbc1(jdbcDriver, jdbcUrl, jdbcUser, jdbcPassword)  // connect to mysql

    // begin from the the offsets committed to the database
    val fromOffsets = DB.readOnly{ implicit session =>
      sql"select topic, `partition`, offset from streaming_offset where group_id=$group"
        .map { resultSet =>
          // TopicAndPartition -> offset
          if(null!=resultSet){
            TopicAndPartition(resultSet.string(1), resultSet.int(2)) -> resultSet.long(3)
          }else{
            null
          }
        }.list.apply().toMap
    }

    val stream = createStream(fromOffsets, kafkaParams, conf, ssc, topics)

    stream.foreachRDD { rdd =>
      if(rdd.count != 0){
        // you task
        rdd.map(record => (record, 1)).reduceByKey {_+_}.count()

        // persist the offset into the database
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        DB.localTx { implicit session =>
          offsetRanges.foreach { osr =>
            sql"replace into streaming_offset values(${osr.topic}, ${osr.partition}, ${osr.untilOffset}, ${group})".update.apply()
            if(osr.partition == 0){
              println(osr.partition, osr.untilOffset)
            }
          }
        }
      }
    }
    ssc
  }
}