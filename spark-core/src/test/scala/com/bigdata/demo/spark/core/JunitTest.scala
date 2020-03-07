package com.bigdata.demo.spark.core

import org.junit.{After, AfterClass, Before, BeforeClass, Test}

class JunitTest {

  /**
   * 每个测试方法执行前执行，多个@Before函数，遵循先定义后调用的压栈原则
   */
  @Before
  def before1(): Unit ={
    println("before1")
  }

  @Before
  def before2(): Unit ={
    println("before2")
  }

  /**
   * 每个测试方法执行后执行，多个@After函数，遵循先定义先调用原则
   * 测试样例发生异常也被执行
   */
  @After
  def after1(): Unit ={
    println("after1")
  }

  @After
  def after2(): Unit ={
    println("after2")
  }

  /**
   * 测试方法
   */
  @Test
  def test1(): Unit ={
    println("test")
  }

  @Test
  def error(): Unit ={
    println(1/0)
  }

}

object JunitTest{
  /**
   * 类初始化时执行，且仅执行一次（必须是静态方法)）
   */
  @BeforeClass
  def beforeClass(): Unit ={
    println("beforeClass")
  }

  /**
   * 测试类全部测试方法执行完毕执行，且仅执行一次（必须是静态方法)）
   */
  @AfterClass
  def afterClass(): Unit ={
    println("afterClass")
  }
}
