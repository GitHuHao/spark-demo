package com.bigdata.demo.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.junit.{After, Before, Test}

class SearchFunction(val query:String) extends Serializable {

  def isMatch(str:String):Boolean = {
    str.contains(query)
  }

  def getMatchesFuncRef(rdd:RDD[String]): RDD[String] ={
    // 问题 1：调用了 isMatch，需要传入 this 对象
    rdd.filter(isMatch)
  }

  def getMatchesFieldRef(rdd:RDD[String]):RDD[String] = {
    // 问题 2：调用了 query ，也需要传入 this 对象
    rdd.filter(_.contains(query))
  }

  def getMatchesNoRef(rdd:RDD[String]):RDD[String] = {
    // 安全：通过局部变量接收 成员变量, 且不依赖启用外部函数
    val query_ = this.query
    rdd.filter(_.contains(query_))
  }

}

class RddParam {

  private var conf:SparkConf = null
  private var sc:SparkContext = null

  @Before
  def before(): Unit ={
    conf = new SparkConf().setAppName("rdd-params").setMaster("local[*]")
    sc = new SparkContext(conf)
  }

  @After
  def after(): Unit ={
    sc.stop()
  }

  /**
   * rdd 作为参数传入对象，内部 filter 调用了对象的判定函数，不安全
   */
  @Test
  def funcRef(): Unit ={
    val search = new SearchFunction("female")
    val rdd = sc.parallelize(Seq("female", "male"))
    search.getMatchesFuncRef(rdd).foreach(println(_))
  }

  /**
   * rdd 作为参数传入对象，内部引用了对象的属性，不安全
   */
  @Test
  def fieldRef(): Unit ={
    val search = new SearchFunction("female")
    val rdd = sc.parallelize(Seq("female", "male"))
    search.getMatchesFieldRef(rdd).foreach(println(_))
  }

  /**
   * rdd 作为参数传入对象，内部未使用对象成员变量，和函数，安全（推荐）
   */
  @Test
  def noRef(): Unit ={
    val search = new SearchFunction("female")
    val rdd = sc.parallelize(Seq("female", "male"))
    search.getMatchesNoRef(rdd).foreach(println(_))
  }


}
