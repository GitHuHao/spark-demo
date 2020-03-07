package com.bigdata.demo.spark.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object AverageUdaf extends UserDefinedAggregateFunction{

  // 输入类型元数据
  override def inputSchema: StructType = StructType(StructField("input",LongType)::Nil)

  // 中间缓存元数据
  override def bufferSchema: StructType = StructType(StructField("sum",LongType)::StructField("count",LongType)::Nil)

  // s输出类型
  override def dataType: DataType = DoubleType

  // 用以标记针对给定的一组输入，UDAF是否总是生成相同的结果
  override def deterministic: Boolean = true

  // 初始化缓冲
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L // sum
    buffer(1) = 0L // count
  }

  // 分区元素添加到缓冲区
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(!input.isNullAt(0)){
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  // 分区间合并缓冲区
  override def merge(merged: MutableAggregationBuffer, newBuf: Row): Unit = {
    merged(0) = merged.getLong(0) + newBuf.getLong(0)
    merged(1) = merged.getLong(1) + newBuf.getLong(1)
  }

  // 计算最终结果
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0) / buffer.getLong(1).toDouble
  }
}
