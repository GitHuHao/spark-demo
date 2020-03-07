package com.bigdata.demo.spark.core

import org.apache.spark.Partitioner

class RandomPartitioner(partitions:Int) extends Partitioner{
  override def numPartitions: Int = partitions
  override def getPartition(key: Any): Int = s"${key.toString}:${Math.random()}".hashCode.abs % partitions
}
