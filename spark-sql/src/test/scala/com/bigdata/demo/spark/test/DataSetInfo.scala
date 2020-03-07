package com.bigdata.demo.spark.test

import com.bigdata.demo.spark.model.Person
import com.bigdata.demo.spark.udaf.{EmpSalaryAverage, Employee}
import com.bigdata.demo.spark.util.CommonSuit
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Dataset, SparkSession}
import org.junit.Test

class DataSetInfo {

  /**
   * 直接读取 json 文件，借助 as[xxx] 创建 Dataset,默认将 整型全部识别为 long, 字符型全部识别为 string,浮点全部为 double
   * 转换为对应样例类对象时，用 withColumn 转换类型
   */
  @Test
  def toDS(): Unit ={
    val conf = new SparkConf().setAppName("spark-sql").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    try {

      spark.read.format("json")
        .option("mode", "OVERWRITE")
        .option("dateFormat", "yyyy-MM-dd")
        .option("path", CommonSuit.getFile("json/"))

      /**
       * 直接读取 json 文件，借助 as[xxx] 创建 Dataset，
       * root
       * |-- age: long (nullable = true)
       * |-- id: long (nullable = true)
       * |-- name: string (nullable = true)
       */

      import org.apache.spark.sql.functions._
      val ds: Dataset[Person] = spark.read.json(CommonSuit.getFile("json/"))
        .withColumn("id",col("id").cast(IntegerType))
        .withColumn("age",col("age").cast(IntegerType))
        .as[Person]
      ds.printSchema()
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /**
   * 强类型用户自定义聚合函数
   * 实现接口 Aggregator[Employee,Average,Double] ，参数依次为 入参对象，缓存类型，输出结果类型
   */
  @Test
  def typedAggregator(): Unit ={
    val conf = new SparkConf().setAppName("spark-sql").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    try {
      val emps: Dataset[Employee] = spark.sparkContext.parallelize(Seq(Employee("a", 100l), Employee("b", 200l), Employee("c", 200l))).toDS()

      val avgSalary = new EmpSalaryAverage().toColumn.name("avg_salary")

      val result = emps.select(avgSalary)

      result.show()

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }

  }






}
