package com.bigdata.demo.spark.test

import com.bigdata.demo.spark.util.CommonSuit
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.Test

class ParquetInfo {

  /**
   * spark 默认输出文件格式为 parquet，通过设置 spark.sql.sources.default 定义修改 (json,csv,parquet,jdbc,libsvm,orc,text)
   * 直接查 json
   */
  @Test
  def defaultSave(): Unit ={
    val conf = new SparkConf().setAppName("spark-sql").setMaster("local[*]").set("spark.sql.sources.default","json")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val jsonPath = CommonSuit.getFile("json/part-00000-ae44cf54-348f-4d2e-a118-8f32bee0a844.json")
    try {
      val df = spark.sql(s"select * from json.`${jsonPath}`")
      df.show()

      val json2Path = "/Users/huhao/softwares/idea_proj/spark-demo/spark-sql/src/main/resources/json2"

      CommonSuit.deleteLocalDir(json2Path)
      df.repartition(1).write.save(json2Path)
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /**
   * spark 读取 parquet 文件，默认自动合并 schema
   */
  @Test
  def mergeParquetSchema(): Unit ={
    val conf = new SparkConf().setAppName("spark-sql").setMaster("local[*]")
      .set("spark.sql.parquet.mergeSchema","true") // 合并 parquet 文件 schema
    val spark = SparkSession.builder().config(conf).getOrCreate()

    try {
      val parquetPath = "/Users/huhao/softwares/idea_proj/spark-demo/spark-sql/src/main/resources/parquet"
      CommonSuit.deleteLocalDir(parquetPath)

      val rdd = spark.sparkContext.parallelize(Seq("""{"name":"jack","age1":20}""", """{"title":"A Good Book","price":23,"age1":20}"""))
      spark.read.json(rdd).repartition(2)
        .write.format("parquet")
        .save(parquetPath)

      val df = spark.read.load(parquetPath)
      df.printSchema()
      /**
       * root
       * |-- age: long (nullable = true)
       * |-- name: string (nullable = true)
       * |-- price: long (nullable = true)
       * |-- title: string (nullable = true)
       */
      df.show()
      /**
       * +----+----+-----+-----------+
       * | age|name|price|      title|
       * +----+----+-----+-----------+
       * |  21|jack| null|       null|
       * |null|null|   23|A Good Book|
       * +----+----+-----+-----------+
       */
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  @Test
  def partitionKeyParse(): Unit ={
    val conf = new SparkConf().setAppName("spark-sql").setMaster("local[*]")
      .set("spark.sql.sources.partitionColumnTypeInference.enabled","true") // 关闭之后不再解析 分区间类型，，统一按 string 处理
    val spark = SparkSession.builder().config(conf).getOrCreate()

    try {
      val df = spark.read.format("parquet").load("hdfs://hadoop01:9000/user/hive/warehouse/person_parquet")
      df.printSchema()

      /**
       * partitionColumnTypeInference = true (默认自动解析分区键)
       * root
       * |-- id: string (nullable = true)
       * |-- name: string (nullable = true)
       * |-- month: integer (nullable = true)
       *
       * partitionColumnTypeInference = false (停止自动解析分区键，一致按 string 解析)
       * root
       * |-- id: string (nullable = true)
       * |-- name: string (nullable = true)
       * |-- month: string (nullable = true)
       */

      df.show()

      /**
       * +---+----+------+
       * | id|name| month|
       * +---+----+------+
       * |  1|  a1|202002|
       * |  2|  a2|202002|
       * |  3|  a3|202002|
       * |  1|  a1|202002|
       * |  2|  a2|202002|
       * |  3|  a3|202002|
       * +---+----+------+
       */

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }











}
