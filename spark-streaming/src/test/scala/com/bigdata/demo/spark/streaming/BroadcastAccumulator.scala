package com.bigdata.demo.spark.streaming

import java.io.File

import com.bigdata.demo.spark.util.SetupJdbc
import kafka.common.TopicAndPartition
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, State, StreamingContext, Time}
import org.apache.spark.util.LongAccumulator
import org.junit.Test
import scalikejdbc._

/**
 * 定义广播变量，传入 SparkContext 注册
 */
object WordBlacklist {

  // 每次都直接读内存，跳过 cpu cache(保证每次读到的都是最新的)
  @volatile private var instance: Broadcast[Seq[String]] = null

  // 单例 双端检索
  def getInstance(sc: SparkContext): Broadcast[Seq[String]] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val wordBlacklist = Seq("a", "b", "c")
          instance = sc.broadcast(wordBlacklist)
        }
      }
    }
    instance
  }
}

/**
 * 定义累加器，传入 SparkContext 注册
 */
object DroppedWordsCounter {

  @volatile private var instance: LongAccumulator = null

  def getInstance(sc: SparkContext): LongAccumulator = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.longAccumulator("WordsInBlacklistCounter")
          SetupJdbc()
          val lastValue = DB.readOnly{ implicit session =>
            sql"select result from accumulator where id=1"
              .map{resultSet =>
                resultSet.long(1)
              }.single.apply().getOrElse(0l)
          }
          println(s"accumulator init data: ${lastValue}")
          instance.add(lastValue)
        }
      }
    }
    instance
  }
}


class StreamingAcc {

  @Test
  def test1(): Unit ={
    SetupJdbc()
    val lastValue = DB.readOnly{ implicit session =>
       sql"select result from accumulator where id=1".map{resultSet =>
         resultSet.long(1)
        }.single().apply().getOrElse(0l)
    }
    println(lastValue)
  }

  @Test
  def test(): Unit = {
    val name = "streaming-acc"
    val checkpointPath = new File(name).getAbsolutePath
    val batchDuration = Seconds(1)
    val checkpointInterval = batchDuration * 5
    var droppedWordAcc:LongAccumulator = null

    val updateState = (seq:Seq[Int],state:Option[Int]) => {
      val increment = seq.foldLeft(0)(_ + _)
      val previous = state.getOrElse(0)
      Some(increment+previous)
    }

    val createStreamingContext = ()=> {
      val conf = new SparkConf().setMaster("local[*]").setAppName(name)
      val ssc = new StreamingContext(conf, batchDuration)

      ssc.checkpoint(checkpointPath)

      val stream = ssc.socketTextStream("localhost", 4000)
        .flatMap(_.split("\\s+"))
        .map((_, 1))
        .reduceByKey(_ + _)

      val filteredStream = stream.mapPartitions{iter =>
        // DroppedWordsCounter,WordBlacklist 遵循单例模式，且必须放在转换算子，才能保证从checkpoint中恢复时，能正常工作
        droppedWordAcc = DroppedWordsCounter.getInstance(SparkContext.getOrCreate(conf))
        val blacklist = WordBlacklist.getInstance(SparkContext.getOrCreate(conf))
        iter.filter{case (k,v) =>
          if(blacklist.value.contains(k)){
            droppedWordAcc.add(v)
            SetupJdbc()
            DB.localTx { implicit session =>
              sql"replace into accumulator(id,result) values(1,${droppedWordAcc.value})".update.apply()
            }
            false
          }else{
            true
          }
        }
      }

      val updatedStream = filteredStream.updateStateByKey(updateState)

      updatedStream.checkpoint(checkpointInterval)

      updatedStream.print()
      ssc
    }

    val ssc = StreamingContext.getOrCreate(checkpointPath, createStreamingContext)

    try {
      ssc.start()
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      ssc.awaitTermination()
    }
  }






}