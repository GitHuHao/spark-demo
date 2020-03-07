package com.bigdata.demo.spark.util

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.junit.Test
import org.json4s.Xml._

case class Student(name:String,age:Int)

case class School(students:List[Student])

class Json4sSuitTest {

  /**
   * 解析 json 字符串，并提取指定一级字段
   */
  @Test
  def parsed(): Unit = {
    implicit val formats = Serialization.formats(ShortTypeHints(List()))
    val value = parse("""{"name":"tom","price":35.35}""",useBigDecimalForDouble = true)
    val name = (value \ "name").extract[String]
    val price = (value \ "price").extract[Double]
    println(value)
    println(s"name: ${name}, age: ${price}")
  }

  /**
   * 逐层解析 json
   * 循环解析 json，直到遇到指定 key
   */
  @Test
  def nested(): Unit ={
    implicit val formats = Serialization.formats(ShortTypeHints(List()))
    val value = parse("""{"name":{"first":"Li","middle":"Ming","last":"Ming"},"price":35.35}""",useBigDecimalForDouble = true)
    val first = (value \ "name" \ "first").extract[String]
    val middle = (value \\ "middle").extract[String]
    println(s"first-name: ${first}, middle-name: ${middle}")
  }

  /**
   * 解析 json 数组
   */
  @Test
  def array(): Unit ={
    implicit val formats = Serialization.formats(ShortTypeHints(List()))
    val value = parse("""{"numbers":[1,2,3,4]}""")
    val numbers:List[BigInt] = for(JArray(elem)<-value;JInt(num)<-elem) yield num
    println(numbers.mkString(","))
  }

  /**
   * for 循环遍历节点，找出同时存在指定字段的节点，然后封装成集合带出
   */
  @Test
  def listOfJson(): Unit ={
    implicit val formats = Serialization.formats(ShortTypeHints(List()))
    val value = parse("""{"name":"Joe","children":[{"name":"Mary","age":5},{"name":"Lily","age":6},{"name":"Lucy","age":7}]}""")
    // 提取所有一级 key
    val nodes = for(JObject(child) <-value) yield child

    // 遍历节点，提取包含 name 和 age 字段的节点
    // (Mary,5),(Lily,6),(Lucy,7)
    val tuples1:List[(String,BigInt)] = for {
      JObject(child) <- value
      JField("name", JString(name)) <- child
      JField("age", JInt(age)) <- child
    } yield (name,age)
    println(tuples1.mkString(","))

    // 遍历节点，提取包含 name 和 age 字段，并且满足 if 条件的节点
    // (Lucy,7)
    val tuples2:List[(String,BigInt)] = for{
      JObject(child) <- value
      JField("name",JString(name)) <- child
      JField("age",JInt(age)) <- child
      if age >6
    } yield (name,age)
    println(tuples2.mkString(","))
  }

  /**
   * 基于样例类提取
   * 基于嵌套样例类提取
   */
  @Test
  def extractFromCaseClass(): Unit ={
    implicit val formats = Serialization.formats(ShortTypeHints(List()))
    val value:List[Student] = parse("""[{"name":"tom","age":22},{"name":"Lily","age":23},{"name":"Lucy","age":20}]""").extract[List[Student]]
    println(value.mkString(","))

    val school = parse("""{"students":[{"name":"tom","age":22},{"name":"Lily","age":23},{"name":"Lucy","age":20}]}""").extract[School]
    println(school)
  }

  @Test
  def dumpsAndLoads(): Unit ={
    // import org.json4s.JsonDSL._
    val data = List(1,2,3)
    val str1:String = compact(render(data))
    println(str1)

    // {"name":"tom","age":20}
    val kvs = ("name"->"tom") ~ ("age"->20) ~ ("gender" -> (None:Option[Int]))
    val str2:String = compact(render(kvs))
    println(str2)

    // 对象 dumps
    val students:List[Student] = List(Student("tom",21),Student("lily",22),Student("lucy",23))
    val school = School(students)
    val str3:String = compact(render(school.students.map(x=>("name"->x.name)~("age"->x.age))))
    println(str3)

    // 美化输出
    val str4 = pretty(render(school.students.map(x=>("name"->x.name)~("age"->x.age))))
    println(str4)

    // loads 对象
    implicit val formats = Serialization.formats(ShortTypeHints(List()))
    val students2 = parse(str3).extract[List[Student]]
    println(students2.mkString(","))

  }

  @Test
  def jsonMerge(): Unit ={
    implicit val formats = Serialization.formats(ShortTypeHints(List()))

    val t1 = parse("""{"name":"Jack","age":21}""")
    val t2 = parse("""{"name":"Tom","score":98.7}""",useBigDecimalForDouble = true)

    val t3 = t1 merge t2
    println(pretty(render(t3)))
  }

  /**
   * 对比 json
   */
  @Test
  def jsonDiff(): Unit ={
    implicit val formats = Serialization.formats(ShortTypeHints(List()))
    val t1 = parse("""{"name":"Jack","age":21}""")
    val t2 = parse("""{"name":"Tom","score":98.7}""",useBigDecimalForDouble = true)
    val Diff(changed,added,deleted) = t1 diff t2

    println(changed)
    println(added)
    println(deleted)

    /**
     * JObject(List((name,JString(Tom))))
     * JObject(List((score,JDecimal(98.7))))
     * JObject(List((age,JInt(21))))
     */
  }

  /**
   * Elem 对象转换为 json 对象
   */
  @Test
  def xmlToJson(): Unit ={
    implicit val formats = Serialization.formats(ShortTypeHints(List()))
    val xml = <users>
      <user>
        <id>1</id>
        <name>Harry</name>
      </user>
      <user>
        <id>2</id>
        <name>David</name>
      </user>
    </users>

    val value = toJson(xml)
    println(pretty(render(value)))

  }

}
