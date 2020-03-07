package com.bigdata.demo.spark.udaf

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator


case class Employee(name:String,salary:Long)

case class Average(var sum:Long,var count:Long) // 需要重复赋值，设置为 var

class EmpSalaryAverage extends Aggregator[Employee,Average,Double]{

  // 分区内部累加器初始化
  override def zero: Average = Average(0l,0l)

  // 分区内部累计
  override def reduce(buffer: Average, emp: Employee): Average = {
    buffer.sum += emp.salary
    buffer.count += 1
    buffer
  }

  // 分区间合并
  override def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  // 最终输出
  override def finish(reduction: Average): Double = {
    reduction.sum / reduction.count.toDouble
  }

  // 缓冲对象编码
  override def bufferEncoder: Encoder[Average] = Encoders.product

  // 最终输出结果编码
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

