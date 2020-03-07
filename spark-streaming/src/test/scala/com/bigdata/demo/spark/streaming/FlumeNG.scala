package com.bigdata.demo.spark.streaming

import java.net.InetSocketAddress

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.junit.Test

class FlumeNG {

  /**
flume 推流到spark （不可停机）
# 声明组件 a1: agent, s1:输入源, k1:输出源, c1 通道
a1.sources = s1
a1.sinks = k1
a1.channels = c1

# 输入源 1 监控本机 44444 端口
a1.sources.s1.type = netcat
a1.sources.s1.bind = 0.0.0.0
a1.sources.s1.port = 4000

# 声明输出源k1 avro 格式输出
a1.sinks.k1.type = avro
a1.sinks.k1.hostname = 192.168.152.1 （部署 spark 程序及其）
a1.sinks.k1.port = 4001

# 声明通道c1,容许缓存 1000 条事件（消息），每 100 条事件（消息）事务提交一次
a1.channels.c1.type = memory
a1.channels.c1.capacity = 1000
a1.channels.c1.transactionCapacity = 100

# 组装组件
a1.sources.s1.channels = c1
a1.sinks.k1.channel = c1

1 启动 spark 监听程序 （监听 hadoop02 4001 端口）
2 启动 flume 代理 flume-ng agent -n a1 -c conf/ -f data/spark/push.conf  -Dflume.root.logger=INFO,console
3 启动 socket 发送端 nc localhost 4000

   *
   *
   */
  @Test
  def push(): Unit = {
    // flume 推送数据到 spark
    val conf = new SparkConf().setAppName("spark-push").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //推送方式: flume向spark发送数据
    val flumeStream = FlumeUtils.createStream(ssc, "192.168.152.1", 4001)
    // avro 格式 数据{"header":xxxxx   "body":xxxxxx}
    flumeStream.flatMap { x =>
      val msg = Bytes.toString(x.event.getBody.array())
      val headers = x.event.getHeaders
      val schema = x.event.getSchema
      msg.split(" ").map((_, 1))
    }.reduceByKey(_ + _)
      .print()
    ssc.start()
    ssc.awaitTermination()
  }

  /**
spark 从 flume avro 数据池拉取数据 （可停机）
a1.sources = r1
a1.channels = c1
a1.sinks = k1

a1.sources.r1.type = netcat
a1.sources.r1.bind = 0.0.0.0
a1.sources.r1.port = 4000

a1.channels.c1.type = memory
a1.channels.c1.capacity = 10000
a1.channels.c1.transactionCapacity = 100

a1.sinks.k1.type = org.apache.spark.streaming.flume.sink.SparkSink
a1.sinks.k1.hostname = 192.168.152.102 (avro 地址，非s)
a1.sinks.k1.port = 4001

a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1

# 数据存储在 flume 机器上启动的 avro 数据池里面，等待spark 来拉去
注意 可能需要替换flume lib 目录下 spark-streaming-flume-sink_xxxx.jar、scala-library-xxx.jar，与 spark streaming-app 中使用的一致

1 启动 spark 监听程序 （监听 hadoop02 4001 端口）
2 启动 flume 代理 flume-ng agent -n a1 -c conf/ -f data/spark/push.conf  -Dflume.root.logger=INFO,console
3 启动 socket 发送端 nc localhost 4000
   */
  @Test
  def pull(): Unit ={
    val conf = new SparkConf().setAppName("spark-pull").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val address=Seq(new InetSocketAddress("192.168.152.102",4001))

    try {
      val flumeStream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc, address, StorageLevel.MEMORY_AND_DISK_SER_2)
      flumeStream.flatMap { x =>
        val msg = Bytes.toString(x.event.getBody.array())
        val headers = x.event.getHeaders
        val schema = x.event.getSchema
        msg.split(" ").map((_, 1))
      }.reduceByKey(_ + _)
        .print()

      ssc.start()
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      ssc.awaitTermination()
    }


  }



}
