package com.bigdata.demo.spark.util

import sys.process._
import java.net.URI
import java.sql.{Connection, PreparedStatement, ResultSet}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

object CommonSuit {

  def getLogger(any:Any): Logger ={
    LoggerFactory.getLogger(any.getClass)
  }

  def parseArgs(args: Array[String]):Map[String,String]={
    val status = null!=args && args.length>0
    val arg = if(status) args.mkString("") else ""
    val params = arg.split(",").map{x=>
      val kvs = x.stripMargin.split(":")
      if(kvs.length==2){
        (kvs(0).stripMargin->kvs(1).stripMargin)
      }else{
        ("" -> "")
      }
    }.toMap
    params
  }

  def deleteLocalDir(path:String): Boolean ={
    val code = s"rm -rf ${path}".!
    code == 0
  }

  def deleteHdfsDir(path:String, url:String ="hdfs://hadoop01:9000", user:String="admin"): Boolean ={
    val dfsConf = new Configuration()
    val fs = FileSystem.get(new URI(url), dfsConf, user)
    fs.delete(new Path(path), true)
  }

  def getFile(file:String): String ={
    val path = this.getClass.getClassLoader.getResource(file).getPath
    s"chmod +x ${path}".!
    path
  }

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
      conn.commit()
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
      conn.commit()
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

  def jdbcExecute(sql:String,params:Seq[Seq[Any]],batch:Long=1024): Array[Long] ={
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
      conn.commit()
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
