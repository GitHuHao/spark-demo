package com.bigdata.demo.spark.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import com.datastax.driver.core.exceptions.ConnectionException
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class SocketReceiver(host:String,port:Int)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  private var socket:Socket = null
  private var reader:BufferedReader = null

  override def onStart(): Unit = {
    // 创建子线程，接收数据
    new Thread("Socket Receiver"){
      override def run(): Unit = receive()
    }.start()
  }

  override def onStop(): Unit = {
    // 终止操作
    if(null!=reader){
      reader.close()
    }
    if(null!=socket){
      socket.close()
    }
  }

  private def receive(): Unit ={
    try {
      socket = new Socket(host, port)
      reader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))
      var line = reader.readLine()
      while (!isStopped() && null != line) {
        store(line)
        line = reader.readLine()
      }
      reader.close()
      socket.close()
      restart("Trying reconnect ... ") // 服务端没有开启时，重复监听
    } catch {
          // 发生异常重复监听
      case e:ConnectionException => restart(s"Exception occured when try to connect sever '${host}:${port}', ${e.getMessage}")
      case t:Throwable => restart(s"Exception occured when receive data, ${t.getMessage}")
    }

  }


}
