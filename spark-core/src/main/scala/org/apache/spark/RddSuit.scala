package org.apache.spark

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object RddSuit {

  /**
   * ClassTag 类标签
   * @param sc
   * @param path
   * @tparam T
   * @return
   */
  def getCheckpointRdd[T: ClassTag](sc:SparkContext,path:String) ={
    val result : RDD[T] = sc.checkpointFile(path)
    result
  }

  def getCheckpointRdd2[T: ClassTag](path:String)(implicit sc : org.apache.spark.SparkContext, mf : scala.reflect.Manifest[T]) ={
    val result : RDD[T] = sc.checkpointFile(path)
    result
  }

}
