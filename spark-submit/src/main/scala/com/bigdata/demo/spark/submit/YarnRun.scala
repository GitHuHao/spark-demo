package com.bigdata.demo.spark.submit

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import scala.collection.mutable.{HashMap => MHashMap}


object YarnRun {

  val logger = Logger.getLogger(getClass.getSimpleName)

  def argsParse(args: Array[String]): MHashMap[String,String] ={
    val paramMap = new MHashMap[String,String]
    if(null!=args && args.length>0){
      for(i <- 0.until(args.length,2)){
        args(i)
        paramMap.put(args(i),args(i+1))
      }
    }
    logger.debug(s"params: ${paramMap}")
    paramMap
  }

  /**
   * 1）local[n],local[*]模式
   *  使用 client 本地资源运行 spark
   *
   * 2) spark://hadoop01:7077,hadoop02:7077 模式
   *    使用 spark 集群运行（可以非常方便查看日志,DAG,eventLog）
   *
   * 3) yarn 模式运行(直接在 yarn 集群运行 spark 程序，无需启动 spark 集群)
   * 3.1-client 提交(driver程序运行在 client节点，即提交节点，适合调试，能直接接收各种日志，与 client 节点连接中断，任务立即被终止)
   * （日志直接在提交节点查看）
   * spark-submit \
   * --class com.bigdata.demo.spark.submit.YarnRun \
   * --master yarn \
   * --deploy-mode client \
   * --num-executors 2 \
   * --executor-memory 512MB \
   * --executor-cores 1 \
   * --jars /mnt/hgfs/share/spark-submit-jar-with-dependencies.jar \
   * /mnt/hgfs/share/spark-submit.jar \
   * app YarnRun in hdfs://hadoop01:9000/apps/mr/wc/in/
   *
   * 3.2-cluster 提交（driver程序运行在 am，即 application-manager 上，日志需要登录各节点才能查看，与 client 节点连接断开，任务立即终止）
   * (日志在 yarn 集群查看，executor 节点日志)
   * spark-submit \
   * --class com.bigdata.demo.spark.submit.YarnRun \
   * --master yarn \
   * --deploy-mode cluster \
   * --num-executors 2 \
   * --executor-memory 512MB \
   * --executor-cores 1 \
   * --jars /mnt/hgfs/share/spark-submit-jar-with-dependencies.jar \
   * /mnt/hgfs/share/spark-submit.jar \
   * app YarnRun in hdfs://hadoop01:9000/apps/mr/wc/in/
   */

  def main(args: Array[String]): Unit = {
    // 线上提交 yarn 集群使用
    // 线上调试使用 yarn client 模式，方便查看日志，
    // 线上正式环境运行，使用 yarn-cluster 模式，driver日志去 yarn 集群查看,executor 日志去tracking URL: http://hadoop03:8088/proxy/application_1580476692047_0012/）
    logger.setLevel(Level.DEBUG)
    val params = argsParse(args)
    val conf = new SparkConf().setAppName(params.getOrElse("app",getClass.getSimpleName))
    val sc = new SparkContext(conf)
    try {
      sc.textFile(params.getOrElse("in", "hdfs://hadoop01:9000/apps/mr/wc/in/*"))
        .flatMap(_.split(params.getOrElse("split", "\\s+")))
        .map((_, 1))
        .reduceByKey(_ + _)
        .foreachPartition{x=>
          logger.debug(s"data: ${x.mkString(",")}")
        }
      logger.debug("over~")
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      sc.stop()
    }
  }

}
