package com.bigdata.demo.spark.udtf

import java.util
import org.apache.hadoop.hive.ql.exec.UDFArgumentException
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, PrimitiveObjectInspector, StructObjectInspector}


/**
 * 自定提取 first name ,second name，并分别命名为firstName,secondName
 * John Smith
 * John and Ann White
 * Ted Green
 * Dorothy
 */
class NameExtractUDTF extends GenericUDTF{
  private var stringOI:PrimitiveObjectInspector = null

  @throws[UDFArgumentException]
  override def initialize(args: Array[ObjectInspector]): StructObjectInspector = { // 只有一个入参
    if (args.length != 1) throw new UDFArgumentException("NameParserGenericUDTF() takes exactly one argument")
    // 入参类型必须为 字符串
    if ((args(0).getCategory ne ObjectInspector.Category.PRIMITIVE) && (args(0).asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory ne PrimitiveObjectInspector.PrimitiveCategory.STRING)) throw new UDFArgumentException("NameParserGenericUDTF() takes a string as a parameter")
    // input 输入流
    stringOI = args(0).asInstanceOf[PrimitiveObjectInspector]
    // output
    val fieldNames = new util.ArrayList[String](2)
    val fieldOIs = new util.ArrayList[ObjectInspector](2)
    fieldNames.add("firstName")
    fieldNames.add("secondName")
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    // 绑定 每行 字段名称，字段类型
    ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs)
  }

  def processInputRecord(name: String): util.ArrayList[Array[AnyRef]] = {
    val result = new util.ArrayList[Array[AnyRef]]
    // ignoring null or empty input
    if (name == null || name.isEmpty) return result
    val tokens = name.split("\\s+")
    if (tokens.length == 2) result.add(Array[AnyRef](tokens(0), tokens(1)))
    else if (tokens.length == 4 && tokens(1) == "and") {
      result.add(Array[AnyRef](tokens(0), tokens(3)))
      result.add(Array[AnyRef](tokens(2), tokens(3)))
    }
    result
  }

  @throws[HiveException]
  override def process(record: Array[AnyRef]): Unit = { // 遍历
    val name = stringOI.getPrimitiveJavaObject(record(0)).toString
    val results = processInputRecord(name)
    val it = results.iterator
    while ( {
      it.hasNext
    }) {
      val r = it.next
      forward(r)
    }
  }

  @throws[HiveException]
  override def close(): Unit = {
    // do nothing
  }

}
