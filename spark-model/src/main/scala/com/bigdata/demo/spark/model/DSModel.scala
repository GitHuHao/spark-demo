package com.bigdata.demo.spark.model

case class TbStock(ordernumber:String,locationid:String,dateid:String) extends Serializable

case class TbStockDetail(ordernumber:String, rownum:Int, itemid:String, number:Int, price:Double, amount:Double) extends Serializable

case class TbDate(dateid:String, years:Int, theyear:Int, month:Int, day:Int, weekday:Int, week:Int, quarter:Int, period:Int, halfmonth:Int) extends Serializable