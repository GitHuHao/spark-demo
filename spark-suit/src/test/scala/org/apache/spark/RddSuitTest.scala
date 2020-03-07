package org.apache.spark

import org.apache.spark.rdd.RDD
import org.junit.Test

class RddSuitTest {

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

}
