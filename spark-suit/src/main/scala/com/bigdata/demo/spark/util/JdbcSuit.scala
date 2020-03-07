package com.bigdata.demo.spark.util

import java.sql.{Connection, PreparedStatement, ResultSet}

import scala.collection.mutable.ListBuffer
import scala.collection.mutable
import com.typesafe.config.ConfigFactory
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import scalikejdbc._

object JdbcSuit {

  /**
    * old-version: com.mysql.jdbc.Driver
    * new-version: com.mysql.cj.jdbc.Driver
    * @param driverClass
    * @param url
    * @param user
    * @param password
    * @return
    */
  def getJdbcConnection(driverClass:String="com.mysql.cj.jdbc.Driver",
                        url:String="jdbc:mysql://hadoop01:3306/company",
                        user:String="hive",
                        password:String="hive"): Connection ={

    Class.forName(driverClass).newInstance()
    java.sql.DriverManager.getConnection(url,user,password)
  }

  def parseToMap(resultSet:ResultSet): ListBuffer[Map[String,Any]]= {
    val metaData = resultSet.getMetaData()
    val count = metaData.getColumnCount
    val result = new ListBuffer[Array[Any]]
    val columns = (1 to count).map(x=>metaData.getColumnName(x)).toList
    while(resultSet.next()){
      result.append((1 to count).map(x=>resultSet.getObject(x)).toArray)
    }
    result.map(x=> (columns zip x).toMap)
  }

  def parseToSeq(resultSet:ResultSet): ListBuffer[Seq[Any]]= {
    val metaData = resultSet.getMetaData()
    val count = metaData.getColumnCount
    val result = new ListBuffer[Seq[Any]]
    val columns = (1 to count).map(x=>metaData.getColumnName(x)).toList
    while(resultSet.next()){
      result.append((1 to count).map(x=>resultSet.getObject(x)))
    }
    result
  }

  def jdbcQueryToSeq(sql:String,params:Any*): ListBuffer[Seq[Any]]= {
    var conn: Connection = null
    var ps: PreparedStatement = null
    try {
      conn = getJdbcConnection()
      ps = conn.prepareStatement(sql)
      if(null!=params){
        var i = 0
        params.foreach{x=>
          i += 1
          ps.setObject(i,x)
        }
      }
      val resultSet = ps.executeQuery()
      parseToSeq(resultSet)
    } catch {
      case e:Exception => throw e
    }finally {
      if (null != ps) {
        ps.close()
      }
      if (null != conn) {
        conn.close()
      }
    }
  }

  def jdbcQueryToMap(sql:String,params:Any*): ListBuffer[Map[String,Any]]= {
    var conn: Connection = null
    var ps: PreparedStatement = null
    try {
      conn = getJdbcConnection()
      ps = conn.prepareStatement(sql)
      if(null!=params){
        var i = 0
        params.foreach{x=>
          i += 1
          ps.setObject(i,x)
        }
      }
      val resultSet = ps.executeQuery()
      parseToMap(resultSet)
    } catch {
      case e:Exception => throw e
    } finally {
      if (null != ps) {
        ps.close()
      }
      if (null != conn) {
        conn.close()
      }
    }
  }

  def jdbcExecute(sql:String): Int ={
    var conn: Connection = null
    var ps: PreparedStatement = null
    try {
      conn = getJdbcConnection()
      ps = conn.prepareStatement(sql)
      val effected = ps.executeUpdate()
      effected
    } catch {
      case e: Exception => throw e
    } finally {
      if (null != ps) {
        ps.close()
      }
      if (null != conn) {
        conn.close()
      }
    }
  }

  def jdbcExecute(sql:String,params:Any*): Int ={
    var conn: Connection = null
    var ps: PreparedStatement = null
    try {
      conn = getJdbcConnection()
      ps = conn.prepareStatement(sql)
      var i = 0
      params.foreach{x=>
        i += 1
        ps.setObject(i,x)
      }
      val effected = ps.executeUpdate()
      effected
    } catch {
      case e: Exception => throw e
    } finally {
      if (null != ps) {
        ps.close()
      }
      if (null != conn) {
        conn.close()
      }
    }
  }

  def jdbcBatchExecute(sql:String,params:Seq[Seq[Any]],batch:Long=1024): Array[Long] ={
    var conn: Connection = null
    var ps: PreparedStatement = null
    try {
      conn = getJdbcConnection()
      ps = conn.prepareStatement(sql)
      val result = new ListBuffer[Array[Long]]
      var rec = 0
      val totalRec = params.length
      params.foreach{ once =>
        rec += 1
        var i = 0
        once.foreach{x=>
          i += 1
          ps.setObject(i,x)
        }
        ps.addBatch()
        if(rec%batch==0 || rec==totalRec){
          val effectedArr = ps.executeLargeBatch()
          result.append(effectedArr)
        }
      }
      result.flatten.toArray
    } catch {
      case e: Exception => throw e
    } finally {
      if (null != ps) {
        ps.close()
      }
      if (null != conn) {
        conn.close()
      }
    }
  }
}

object SetupJdbc {
  def apply(): Unit = {
    val conf = ConfigFactory.load("spark-streaming.conf")
    val driver = conf.getString("jdbc.driver")
    val host = conf.getString("jdbc.url")
    val user = conf.getString("jdbc.user")
    val password = conf.getString("jdbc.password")
    Class.forName(driver)
    ConnectionPool.singleton(host, user, password)
  }
}
