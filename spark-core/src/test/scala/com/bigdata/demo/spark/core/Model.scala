package com.bigdata.demo.spark.core

import com.datastax.spark.connector.UDTValue
import com.datastax.spark.connector.types.TypeConverter
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.ShortTypeHints
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.jackson.Serialization
import com.datastax.spark.connector.types._

import scala.reflect.runtime.universe._

/**
  * RDD 传递的对象需要实现序列化接口 和 Ordered 接口
 *
 * @param name
  * @param age
  */
case class Person(name:String,age:Int) extends Ordered[Person] with Serializable{
  override def compare(that: Person): Int = {
    if(this.name.equals(that.name)){
      that.age - that.age
    }else{
      this.name.compareTo(that.name)
    }
  }
}

object Person{

//  def apply(name: String, age: Int): Person = new Person(name, age)

  def apply(json: String): Person ={
    implicit val formats = Serialization.formats(ShortTypeHints(List()))
    val value = parse(json,useBigDecimalForDouble = true)
    val name = (value \ "name").extract[String]
    val age = (value \ "age").extract[Int]
    new Person(name,age)
  }

  def loads(json: String): Person ={
    implicit val formats = Serialization.formats(ShortTypeHints(List()))
    val value = parse(json,useBigDecimalForDouble = true)
    val name = (value \ "name").extract[String]
    val age = (value \ "age").extract[Int]
    new Person(name,age)
  }

  def dumps(person: Person): String ={
    compact(render(("name"->person.name) ~ ("age"->person.age)))
  }
}

case class Score(chinese:Int,english:Int,math:Int) extends Ordered[Score] with Serializable{
  override def compare(that: Score): Int = {
    if(this.chinese.equals(that.chinese)){
      that.chinese - that.chinese
    }else if(this.english.equals(that.english)){
      that.english - that.english
    }else{
      that.math - that.math
    }
  }
}

object ScoreTypeConverter extends TypeConverter[Score] {
  override def targetTypeTag:TypeTag[Score] =  typeTag[Score]

  override def convertPF: PartialFunction[Any, Score] = {
    case data:UDTValue=> Score(data.getInt("chinese"),data.getInt("english"),data.getInt("math"))
    case _ => null
  }
}





