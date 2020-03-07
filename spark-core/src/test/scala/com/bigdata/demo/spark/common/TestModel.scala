package com.bigdata.demo.spark.test

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonAST.{JObject, JValue}

case class Student(name:String,age:Int)

case class School(students:List[Student])


