package com.bigdata.demo.spark.util

import org.junit.Test


class JdbcSuitTest {

  @Test
  def select(): Unit ={
    val buffer1 = JdbcSuit.jdbcQueryToSeq("select * from book")
    buffer1.foreach(x=>println(x.mkString(",")))

//    val buffer2 = JdbcSuit.jdbcQueryToSeq("select * from book where id>=? and id<=?",1,2)
//    buffer2.foreach(x=>println(x.mkString(",")))
//
//    val maps1 = JdbcSuit.jdbcQueryToMap("select * from book")
//    maps1.foreach(println(_))
//
//    val maps2 = JdbcSuit.jdbcQueryToMap("select * from book where id>=? and id<=?",1,2)
//    maps2.foreach(println(_))
  }

  @Test
  def insert(): Unit ={
    val effected1 = JdbcSuit.jdbcExecute("insert into book(name,price) value('A1',10)")
    println(effected1)

    val effected2 = JdbcSuit.jdbcExecute("insert into book(name,price) values('A1',10),('A2',20)")
    println(effected2)

    val effected3 = JdbcSuit.jdbcExecute("insert into book(name,price) values(?,?)","A3",30)
    println(effected3)

    val effected4 = JdbcSuit.jdbcBatchExecute("insert into book(name,price) values(?,?)",Seq(Seq("A4",40),Seq("A5",50)))
    println(effected4.mkString(","))

  }

  @Test
  def delete(): Unit ={
    val num1 = JdbcSuit.jdbcExecute("delete from book where id=4")
    println(num1)

    val num2 = JdbcSuit.jdbcExecute("delete from book where id=?",5)
    println(num2)

    val num3 = JdbcSuit.jdbcBatchExecute("delete from book where id=?",Seq(Seq(6),Seq(7)))
    println(num3.mkString(","))
  }

  @Test
  def update(): Unit ={
    val num1 = JdbcSuit.jdbcExecute("update book set name=lower(name) where id=4")
    println(num1)

    val num2 = JdbcSuit.jdbcExecute("update book set name=lower(name) where id=?",10)
    println(num2)

    val longs = JdbcSuit.jdbcBatchExecute("update book set name=lower(name) where id=?",Seq(Seq(11),Seq(10)))
    println(longs.mkString(","))
  }




}
