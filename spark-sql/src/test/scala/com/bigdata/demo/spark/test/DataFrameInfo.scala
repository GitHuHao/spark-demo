package com.bigdata.demo.spark.test


import java.util.{Date, Properties}

import org.junit.Test
import com.bigdata.demo.spark.udaf.AverageUdaf
import com.bigdata.demo.spark.util.CommonSuit
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, current_date, current_timestamp, date_format}
import org.apache.spark.sql.types.{DateType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}

class DataFrameInfo {

  /**
   * rdd  通过 toDF 得到 dateframe
   * toDF() : org.apache.spark.sql.DataFrame
   * toDF(colNames : scala.Predef.String*) : org.apache.spark.sql.DataFrame
   */
  @Test
  def toDF(): Unit = {
    val conf = new SparkConf().setAppName("spark-sql").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    try {
      // rdd 转换为 dataframe 只支持 数值类型和字符串类型，不支持字符串
      // java.lang.UnsupportedOperationException: No Encoder found for Char
      val rdd = spark.sparkContext.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val df1:DataFrame = rdd.toDF()
      println(df1.columns.mkString(","))
      df1.show()
      df1.select("_1","_2").show()
      /**
       * 不指定列名，基于下标引用 _index
       * _1,_2
       * +---+---+
       * | _1| _2|
       * +---+---+
       * |  a|  1|
       * |  b|  2|
       * |  c|  3|
       * +---+---+
       */

      val df2 = rdd.toDF("tag", "version")
      println(df2.columns.mkString(","))
      df2.show()
      df2.select("tag","version").show()
      /**
       * 指定列名，直接基于列名引用
       * tag,version
       * +---+-------+
       * |tag|version|
       * +---+-------+
       * |  a|      1|
       * |  b|      2|
       * |  c|      3|
       * +---+-------+
       */
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  @Test
  def readCsv(): Unit ={
    val conf = new SparkConf().setAppName("sql-spark").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    try {
      spark.read
        .csv(CommonSuit.getFile("csv/person.csv"))
        .toDF("name", "age") // 手动指定 columns
        .show()
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  @Test
  def readJson(): Unit ={
    val conf = new SparkConf().setAppName("sql-spark").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    try {
      spark.read
        .json(CommonSuit.getFile("json/people.json"))
        .toDF("name1","age1") // 手动指定 columns 替换之前的
        .show()
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /**
   * jdbc(url: String, table: String, properties: Properties): DataFrame =
   */
  @Test
  def readJdbc1(): Unit ={
    val conf = new SparkConf().setAppName("sql-spark").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    try {
      /**
       * 避免 jdbc 抓取不均匀
       * 方案 1 设置 partitionColumn 分区键、lowerBound 跨分区下界、upperBound 跨分区上界、numPartitions 理论分区数
       *    实际分区数：partitions = min(numPartitions,(upperBound-lowerBound))
       */

      // 分区数：partitions = min(numPartitions,(upperBound-lowerBound)) = min(10,200-0)
      // 分区元素个数区间: [0, min(upperBound/partitions,upperBound)] = [0,min(200/10,200)] = [0, 20]
      val prop = new Properties()
      prop.put("user","hive")
      prop.put("password","hive")
      prop.put("partitionColumn","id")
      prop.put("lowerBound","0")
      prop.put("upperBound","200")
      prop.put("numPartitions","10")

      spark.read
        .jdbc("jdbc:mysql://hadoop01:3306/company","book",prop)
        .select("id","name","price")
        .foreachPartition{x=>
          val list = x.toList
          println(s">>> size: ${list.size} data: ${list.mkString(",")}")
        }
      /**
       * size: 0 data:
       * size: 0 data:
       * size: 0 data:
       * size: 0 data:
       * size: 0 data:
       * size: 15 data: [1,Lie Sporting,30],[2,Pride & Prejudice,70],[3,Fall of Giants,50],[8,A2,20],[9,A3,30],[10,B10,30],[11,B11,31],[12,B12,32],[13,B13,33],[14,B14,34],[15,B15,35],[16,B16,36],[17,B17,37],[18,B18,38],[19,B19,39]
       * size: 11 data: [40,a4,4],[41,a20,20],[42,a29,29],[43,a12,12],[44,a5,5],[45,a21,21],[46,a30,30],[47,a13,13],[48,a6,6],[49,a22,22],[50,a14,14]
       * size: 20 data: [20,a23,23],[21,a7,7],[22,a0,0],[23,a15,15],[24,a24,24],[25,a16,16],[26,a1,1],[27,a8,8],[28,a25,25],[29,a17,17],[30,a2,2],[31,a9,9],[32,a26,26],[33,a18,18],[34,a3,3],[35,a10,10],[36,a27,27],[37,a19,19],[38,a28,28],[39,a11,11]
       * size: 0 data:
       * size: 0 data:
       */

      prop.put("lowerBound","20")
      prop.put("upperBound","28")
      prop.put("numPartitions","10")

      // 分区数：partitions = min(numPartitions,(upperBound-lowerBound)) = min(10,28-20) = 8
      spark.read
        .jdbc("jdbc:mysql://hadoop01:3306/company","book",prop)
        .select("id","name","price")
        .foreachPartition{x=>
          val list = x.toList
          println(s">> size: ${list.size} data: ${list.mkString(",")}")
        }

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  @Test
  def readJdbc2(): Unit ={
    val conf = new SparkConf().setAppName("sql-spark").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    try {
      val prop1 = new Properties()
      prop1.put("user","hive")
      prop1.put("password","hive")
      prop1.put("fetchsize","10")
      prop1.put("numPartitions","10")

      /**
       * val JDBC_URL = newOption("url")
       * val JDBC_TABLE_NAME = newOption("dbtable")
       * val JDBC_DRIVER_CLASS = newOption("driver")
       * val JDBC_PARTITION_COLUMN = newOption("partitionColumn")
       * val JDBC_LOWER_BOUND = newOption("lowerBound")
       * val JDBC_UPPER_BOUND = newOption("upperBound")
       * val JDBC_NUM_PARTITIONS = newOption("numPartitions")
       * val JDBC_BATCH_FETCH_SIZE = newOption("fetchsize")
       * val JDBC_TRUNCATE = newOption("truncate")
       * val JDBC_CREATE_TABLE_OPTIONS = newOption("createTableOptions")
       * val JDBC_BATCH_INSERT_SIZE = newOption("batchsize")
       * val JDBC_TXN_ISOLATION_LEVEL = newOption("isolationLevel")
       */
      spark.read
        .jdbc("jdbc:mysql://hadoop01:3306/company","book",prop1)
        .select("id","name","price")
        .foreachPartition{x=>
          val list = x.toList
          println(s">> size: ${list.size} data: ${list.mkString(",")}")
        }

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /**
   * jdbc(
   * url: String,
   * table: String,
   * columnName: String,
   * lowerBound: Long,
   * upperBound: Long,
   * numPartitions: Int,
   * connectionProperties: Properties): DataFrame
   */
  @Test
  def readJdbc3(): Unit ={
    val conf = new SparkConf().setAppName("sql-spark").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    try {
      /**
       * 避免 jdbc 抓取不均匀
       * 方案 1 设置 partitionColumn 分区键、lowerBound 跨分区下界、upperBound 跨分区上界、numPartitions 理论分区数
       *    实际分区数：partitions = min(numPartitions,(upperBound-lowerBound))
       */

      // 分区数：partitions = min(numPartitions,(upperBound-lowerBound)) = min(10,200-0)
      // 分区元素个数区间: [0, min(upperBound/partitions,upperBound)] = [0,min(200/10,200)] = [0, 20]
      val prop = new Properties()
      prop.put("user","hive")
      prop.put("password","hive")

      spark.read
        .jdbc("jdbc:mysql://hadoop01:3306/company","book","id",100l,150,10,prop)
        .select("id","name","price")
        .foreachPartition{x=>
          val list = x.toList
          println(s">> size: ${list.size} data: ${list.mkString(",")}")
        }

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /**
   * predicates 为 WHERE 条件
   * jdbc(
   * url: String,
   * table: String,
   * predicates: Array[String],
   * connectionProperties: Properties)
   */
  @Test
  def readJdbc4(): Unit ={
    val conf = new SparkConf().setAppName("sql-spark").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    try {
      /**
       * 避免 jdbc 抓取不均匀
       * 方案 1 设置 partitionColumn 分区键、lowerBound 跨分区下界、upperBound 跨分区上界、numPartitions 理论分区数
       *    实际分区数：partitions = min(numPartitions,(upperBound-lowerBound))
       */

      // 分区数：partitions = min(numPartitions,(upperBound-lowerBound)) = min(10,200-0)
      // 分区元素个数区间: [0, min(upperBound/partitions,upperBound)] = [0,min(200/10,200)] = [0, 20]
      val prop = new Properties()
      prop.put("user","hive")
      prop.put("password","hive")

      val predicates = 0.to(200,10).map{x=>
        if(x<200){
          s"id>=${x} and id<${x+10}"
        } else{
          s"id>=${x}"
        }
      }.toArray
      println(predicates.mkString(","))

      spark.read
        .jdbc("jdbc:mysql://hadoop01:3306/company","book",predicates,prop)
        .select("id","name","price")
        .foreachPartition{x=>
          val list = x.toList
          println(s">> size: ${list.size} data: ${list.mkString(",")}")
        }

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /**
   * 使用 format 读取 jdbc
   * format(source: String): DataFrameReader
   */
  @Test
  def readFormatJdbc(): Unit ={
    val conf = new SparkConf().setAppName("spark-sql").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    try {
      val df = spark.read.format("jdbc")
        .option("url", "jdbc:mysql://hadoop01:3306/company")
        .option("dbtable", "book")
        .option("user", "hive")
        .option("password", "hive")
        .option("partitionColumn", "id")
        .option("fetchSize", 20)
        .option("numPartitions", 3)
        .option("upperBound", 10)
        .option("lowerBound", 0)
        .load()

      df.select("id", "name", "price").foreachPartition { x =>
        val list = x.toList
        println(s">>> size: ${list.size} data: ${list.mkString(",")}")
      }
      /**
       * >> size: 2 data: [1,Lie Sporting,30],[2,Pride & Prejudice,70]
       * >>> size: 1 data: [3,Fall of Giants,50]
       * >>> size: 43 data: [8,A2,20],[9,A3,30],[10,B10,30],[11,B11,31],[12,B12,32],[13,B13,33],[14,B14,34],[15,B15,35],[16,B16,36],[17,B17,37],[18,B18,38],[19,B19,39],[20,a23,23],[21,a7,7],[22,a0,0],[23,a15,15],[24,a24,24],[25,a16,16],[26,a1,1],[27,a8,8],[28,a25,25],[29,a17,17],[30,a2,2],[31,a9,9],[32,a26,26],[33,a18,18],[34,a3,3],[35,a10,10],[36,a27,27],[37,a19,19],[38,a28,28],[39,a11,11],[40,a4,4],[41,a20,20],[42,a29,29],[43,a12,12],[44,a5,5],[45,a21,21],[46,a30,30],[47,a13,13],[48,a6,6],[49,a22,22],[50,a14,14]
       */

      val df1 = spark.read.format("jdbc")
        .options(Map(
          "url"->"jdbc:mysql://hadoop01:3306/company",
          "dbtable"-> "book",
          "user"->"hive",
          "password"-> "hive",
          "partitionColumn"->"id",
          "fetchSize"->"20",
          "numPartitions"-> "3",
          "upperBound"->"10",
          "lowerBound"-> "0"
        )).load()

      df1.select("id", "name", "price").foreachPartition { x =>
        val list = x.toList
        println(s">> size: ${list.size} data: ${list.mkString(",")}")
      }
      /**
       * >> size: 1 data: [3,Fall of Giants,50]
       * >> size: 2 data: [1,Lie Sporting,30],[2,Pride & Prejudice,70]
       * >> size: 43 data: [8,A2,20],[9,A3,30],[10,B10,30],[11,B11,31],[12,B12,32],[13,B13,33],[14,B14,34],[15,B15,35],[16,B16,36],[17,B17,37],[18,B18,38],[19,B19,39],[20,a23,23],[21,a7,7],[22,a0,0],[23,a15,15],[24,a24,24],[25,a16,16],[26,a1,1],[27,a8,8],[28,a25,25],[29,a17,17],[30,a2,2],[31,a9,9],[32,a26,26],[33,a18,18],[34,a3,3],[35,a10,10],[36,a27,27],[37,a19,19],[38,a28,28],[39,a11,11],[40,a4,4],[41,a20,20],[42,a29,29],[43,a12,12],[44,a5,5],[45,a21,21],[46,a30,30],[47,a13,13],[48,a6,6],[49,a22,22],[50,a14,14]
       */

      println(df1.rdd.getNumPartitions)
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  @Test
  def createRDDWithSchema(): Unit ={
    val conf = new SparkConf().setAppName("spark-sql").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    try {
      val rowRdd = spark.sparkContext
        .textFile(CommonSuit.getFile("text/people.txt"))
        .map { x =>
          val temp = x.split(",")
          Row(temp(0).toInt, temp(1), temp(2).toInt)
        }

      val structType = StructType(Seq(
        StructField("id", IntegerType, false),
        StructField("name", StringType, false),
        StructField("age", IntegerType, false))
      )

      spark.createDataFrame(rowRdd, structType).show()

      /**
       * +---+----+---+
       * | id|name|age|
       * +---+----+---+
       * |  1|jack| 21|
       * |  2| tom| 21|
       * |  3|lucy| 21|
       * +---+----+---+
       */


    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }


  /**
   * 依据 format 读取 jdbc
   */
  @Test
  def jdbcFormat(): Unit ={
    val conf = new SparkConf().setAppName("spark-sql").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val connMap = Map (
      "url"->"jdbc:mysql://hadoop01:3306/company",
      "dbtable"-> "book",
      "user"->"hive",
      "password"-> "hive",
      "partitionColumn"->"id",
      "fetchSize"->"20",
      "numPartitions"-> "3",
      "upperBound"->"10",
      "lowerBound"-> "0"
    )

    try {
      spark.sparkContext.parallelize((1.to(10)).map(i=>(i,s"a${i}",20+i))).toDF("id","name","price")
        .select($"id",$"name",$"price"+10)
        .write
        .mode("overwrite")
        .format("jdbc")
        .options(connMap).save()

     spark.read
       .format("jdbc")
       .options(connMap)
       .load()
       .show()

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }


  /**
   * 依据 format 读取 csv
   * SaveMode: append 追加，overwrite 覆盖,error 如果存储在就报错(默认),ignore 如果存在就忽略
   */
  @Test
  def csvFormat(): Unit ={
    val conf = new SparkConf().setAppName("spark-sql").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    try {
      // rdd + structType 构建 dataframe

      val rdd = spark.sparkContext.parallelize(Seq(Row(1, "tom", 21,"2020-01-02 12:00:00"), Row(2, "jack", 22,"2020-01-02 12:00:00"), Row(3, "lucy", 23,"2020/01/02 12:00:00")))

      val structType = StructType(Seq(
        StructField("id", IntegerType, false),
        StructField("name", StringType, false),
        StructField("age", IntegerType, false),
        StructField("date", DateType, false))
      )

      val df = spark.createDataFrame(rdd, structType)

      val csvPath = "/Users/huhao/softwares/idea_proj/spark-demo/spark-sql/src/main/resources/csv"
      CommonSuit.deleteLocalDir(csvPath)

      // df 存储为 csv 格式
      df.repartition(1).write.format("csv")
        .mode("overwrite") // append 追加，overwrite 覆盖,error 如果存储在就报错(默认),ignore 如果存在就忽略
        .option("header", true) // 保存头
        .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
        .save(csvPath) //

      // spark 读取 csv 格式
      import org.apache.spark.sql.functions._
      val df1 = spark.read.format("csv")
        .option("header", true) // 保存头.load(csvPath).show()
        .load(csvPath)
        .withColumn("id",col("id").cast(IntegerType))
        .withColumn("name",col("id").cast(StringType))
        .withColumn("age",col("age").cast(IntegerType))

      df1.printSchema()
      df1.show()

      /**
       * root
       * |-- id: integer (nullable = true)
       * |-- name: string (nullable = true)
       * |-- age: integer (nullable = true)
       */

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  @Test
  def csvFormat2(): Unit ={
    val conf = new SparkConf().setAppName("spark-sql").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    try {
      val csvPath = "/Users/huhao/softwares/idea_proj/spark-demo/spark-sql/src/main/resources/csv"
      CommonSuit.deleteLocalDir(csvPath)

      val csvSchema = StructType(Seq(
        StructField("id",IntegerType,false),
        StructField("name",StringType,false),
        StructField("date",TimestampType,false))
      )

      val df = spark.read.format("csv")
        .option("header","true")
        .option("delimiter","|")
        .option("mode","FAILFAST")
        .option("inferSchema","false")
        .option("timestampFormat", "yyyy/MM/dd HH:mm:ss") // 自动将 yyyy/MM/dd HH:mm:ss 格式字符串映射称为 timestamp 类型
        .schema(csvSchema)
        .load(csvPath)

      df.printSchema()

      df.show()

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }


  /**
   * 读写 csv
   */
  @Test
  def jsonFormat(): Unit ={
    val conf = new SparkConf().setAppName("spark-sql").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    try {
      // rdd + structType 构建 dataframe
      val rdd = spark.sparkContext.parallelize(Seq(Row(1, "tom", 21), Row(2, "jack", 22), Row(3, "lucy", 23)))

      val structType = StructType(Seq(
        StructField("id", IntegerType, false),
        StructField("name", StringType, false),
        StructField("age", IntegerType, false))
      )

      val df = spark.createDataFrame(rdd, structType)

      val jsonPath = "/Users/huhao/softwares/idea_proj/spark-demo/spark-sql/src/main/resources/json"

      CommonSuit.deleteLocalDir(jsonPath)

      df.repartition(1).write.format("json").save(jsonPath)

      spark.read
        .format("json")
        .load(jsonPath)
        .select($"name",($"age"+10).alias("age")) // 使用 $ 引用字段，alias 取别名
        .show()
      /**
       * +----+---+
       * |name|age|
       * +----+---+
       * | tom| 21|
       * |jack| 22|
       * |lucy| 23|
       * +----+---+
       */
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /**
   * 读写 parquet
   */
  @Test
  def parquetFormat(): Unit ={
    val conf = new SparkConf().setAppName("spark-sql").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    try {
      // rdd + structType 构建 dataframe
      val rdd = spark.sparkContext.parallelize(Seq(Row(1, "tom", 21), Row(2, "jack", 22), Row(3, "lucy", 23)))

      val structType = StructType(Seq(
        StructField("id", IntegerType, false),
        StructField("name", StringType, false),
        StructField("age", IntegerType, false))
      )

      val df = spark.createDataFrame(rdd, structType)

      val parquetPath = "/Users/huhao/softwares/idea_proj/spark-demo/spark-sql/src/main/resources/parquet"

      CommonSuit.deleteLocalDir(parquetPath)

      df.repartition(1)
        .write
        .format("parquet")
        .save(parquetPath)

      spark.read
        .format("parquet")
        .load(parquetPath)
        .show()

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /**
   * spark-sql 内置函数
   * current_timestamp，current_date，date_format
   */
  @Test
  def timestamp(): Unit ={
    val conf = new SparkConf().setAppName("spark-sql").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    try {
      0.until(3).toDF().select(
        current_date().as("current_date"),
        current_timestamp().as("current_timestamp"),
        date_format(current_timestamp(),"yyyy MM dd").as("yyyy MM dd"),
        date_format(current_timestamp(),"yyyy/MM/dd").as("yyyy/MM/dd"),
        date_format(current_timestamp(),"yyyy MMM dd").as("yyyy MMM dd"),
        date_format(current_timestamp(),"yyyy MMMM dd E").as("yyyy MMMM dd E")
      ).show(false) // truncate = false 不截断
      /**
       * +------------+----------------------+----------+----------+-----------+--------------------+
       * |current_date|current_timestamp     |yyyy MM dd|yyyy/MM/dd|yyyy MMM dd|yyyy MMMM dd E      |
       * +------------+----------------------+----------+----------+-----------+--------------------+
       * |2020-02-07  |2020-02-07 12:59:02.07|2020 02 07|2020/02/07|2020 Feb 07|2020 February 07 Fri|
       * |2020-02-07  |2020-02-07 12:59:02.07|2020 02 07|2020/02/07|2020 Feb 07|2020 February 07 Fri|
       * |2020-02-07  |2020-02-07 12:59:02.07|2020 02 07|2020/02/07|2020 Feb 07|2020 February 07 Fri|
       * +------------+----------------------+----------+----------+-----------+--------------------+
       */
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /**
   * filter 过滤
   */
  @Test
  def filter(): Unit ={
    val conf = new SparkConf().setAppName("spark-sql").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    try {
      spark.read
        .format("csv")
        .load(CommonSuit.getFile("csv/person.csv"))
        .toDF("name","age")
        .filter($"age">22)
        .show()
      /**
       * +----+---+
       * |name|age|
       * +----+---+
       * | tom| 21|
       * |jack| 22|
       * |lucy| 23|
       * +----+---+
       */
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /**
   * createTempView 将 df 注册为 session 级别视图，其余 session 包括子 session 无法读取
   */
  @Test
  def createTempView: Unit ={
    val conf = new SparkConf().setAppName("spark-sql").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    try {
      spark.read
        .format("json")
        .load(CommonSuit.getFile("json/people.json"))
        .createTempView("people")

      spark.sql("select * from people").show()
      /**
       * +---+----+
       * |age|name|
       * +---+----+
       * | 21| tom|
       * | 22|lily|
       * | 32|lucy|
       * +---+----+
       */

      spark.newSession().sql("select * from people").show()

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /**
   * createGlobalTempView 将 df 注册为临时全局视图
   * 所有 session 一致从 global_temp 库访问
   *
   */
  @Test
  def createGlobalTempView: Unit ={
    val conf = new SparkConf().setAppName("spark-sql").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    try {
      spark.read
        .format("json")
        .load(CommonSuit.getFile("json/people.json"))
        .createGlobalTempView("people")

      spark.sql("select * from global_temp.people").show()
      /**
       * +---+----+
       * |age|name|
       * +---+----+
       * | 21| tom|
       * | 22|lily|
       * | 32|lucy|
       * +---+----+
       */

      spark.newSession().sql("select * from global_temp.people").show()

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /**
   * createOrReplaceTempView  将 df 注册为 session 级别视图，运行重复注册，后覆盖前
   */
  @Test
  def createOrReplaceTempView: Unit ={
    val conf = new SparkConf().setAppName("spark-sql").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    try {
      spark.read
        .format("json")
        .load(CommonSuit.getFile("json/people.json"))
        .createOrReplaceTempView("people")

      spark.sql("select * from people").show()
      /**
       * +---+----+
       * |age|name|
       * +---+----+
       * | 21| tom|
       * | 22|lily|
       * | 32|lucy|
       * +---+----+
       */

      spark.read
        .format("json")
        .load(CommonSuit.getFile("json/1.json"))
        .createOrReplaceTempView("people")

      spark.sql("select * from people").show()
      /**
       * +---+---+----+--------+
       * |age| id|name|    type|
       * +---+---+----+--------+
       * | 12|  1|tom1|student1|
       * | 12|  2|tom2|student2|
       * | 12|  3|tom3|student3|
       * +---+---+----+--------+
       */

//      spark.newSession().sql("select * from people").show()

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  @Test
  def printSchema(): Unit ={
    val conf = new SparkConf().setAppName("spark-sql").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate() // 当前线程存在 SparkSession 对象，直接使用，否则就创建
    try {
      /**
       * ssv 不指定 StructType 时，全部按 string 处理
       * root
       * |-- _c0: string (nullable = true)
       * |-- _c1: string (nullable = true)
       * |-- _c2: string (nullable = true)
       */
      val df = spark.read.csv(CommonSuit.getFile("csv"))
      df.printSchema()
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /**
   * udf: 一次或多次输入，一个输出
   * lambda 函数注册为 udf 函数
   *
   */
  @Test
  def udf(): Unit ={
    val conf = new SparkConf().setAppName("spark-sql").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate() // 当前线程存在 SparkSession 对象，直接使用，否则就创建

    try {
      val df = spark.read.json(CommonSuit.getFile("json"))

      df.createTempView("people")

      val len = spark.udf.register("len", (x: String) => x.length)

      spark.sql("select id,len(name) as nameLen from people").show()

      import spark.implicits._

      df.select($"id",len($"name").alias("nameLen")).show()

      /**
       * udf
       * +---+----+
       * | id|name|
       * +---+----+
       * |  1| TOM|
       * |  2|JACK|
       * |  3|LUCY|
       * +---+----+
       */

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /**
   * udaf 多对一  多行某列聚合到一行某列（聚合函数）
   * UserDefinedAggregateFunction
   */
  @Test
  def udaf(): Unit ={
    val conf = new SparkConf().setAppName("spark-sql").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate() // 当前线程存在 SparkSession 对象，直接使用，否则就创建

    try {
      spark.read.json(CommonSuit.getFile("json")).createTempView("people")
      spark.udf.register("avg_case",AverageUdaf)
      spark.sql("select avg_case(age) as avg_price from people").show()
      /**
       * udf
       * +---------+
       * |avg_price|
       * +---------+
       * |     22.0|
       * +---------+
       */
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /**
   * udtf 拆分函数，一对多 (1列拆分为多列)
   * 1.enableHiveSupport
   * 2.继承 GenericUDTF 接口，完成参数校验，入参，返回值声明
   * 3.CREATE TEMPORARY FUNCTION func_xx '全类名' USING JAR 'jar path';
   */
  @Test
  def udtf(): Unit ={
    val conf = new SparkConf().setAppName("spark-sql").setMaster("local[*]")
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .config(conf)
      .getOrCreate() // 当前线程存在 SparkSession 对象，直接使用，否则就创建

    import spark.implicits._

    try {
      /**
       * 自动抽取 name 字段的 firstName,secondName
       * 输入
       * John Smith
       * John and Ann White
       * Ted Green
       * Dorothy
       *
       * 输出
       * +---------+----------+
       * |firstname|secondname|
       * +---------+----------+
       * |     John|     Smith|
       * |     John|     White|
       * |      Ann|     White|
       * |      Ted|     Green|
       * +---------+----------+
       */
      spark.sparkContext.parallelize(Seq("John Smith","John and Ann White","Ted Green","Dorothy"))
        .toDF("name")
        .createTempView("movie")

      spark.sql("CREATE TEMPORARY FUNCTION movie_explode as 'com.bigdata.demo.spark.udtf.NameExtractUDTF' USING JAR '/Users/huhao/softwares/idea_proj/spark-demo/spark-sql/target/spark-sql.jar'")
      spark.sql("select movie_explode(name) from movie").show()

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /**
   * 侧写行转列
   */
  @Test
  def latervalView(): Unit ={
    val conf = new SparkConf().setAppName("spark-sql").setMaster("local[*]")
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .config(conf)
      .getOrCreate() // 当前线程存在 SparkSession 对象，直接使用，否则就创建

    import spark.implicits._

    try {
      /**
       * 自动抽取 name 字段的 firstName,secondName
       * 输入
        +---+-----+
        | id| tags|
        +---+-----+
        |  1|a,b,c|
        |  2|  b,c|
        +---+-----+

        +---+-----+---+
        | id| tags|tag|
        +---+-----+---+
        |  1|a,b,c|  a|
        |  1|a,b,c|  b|
        |  1|a,b,c|  c|
        |  2|  b,c|  b|
        |  2|  b,c|  c|
        +---+-----+---+
       */
      spark.sparkContext.parallelize(Seq((1,"a,b,c"),(2,"b,c")))
        .toDF("id","tags")
        .createTempView("data")

      spark.sql("select * from data").show()

      // data lateral view explode(split(tags,',')) as tag 将 表 data 的 tags 列基于 “," 分割，侧写
      spark.sql("select id,tags,tag from data lateral view explode(split(tags,',')) as tag").show()

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /**
   * pivot: 一维数据折叠成二维表
   * 目前只支持 DSL 编码，scala 版本不支持 sql, python 支持 sql
   *  pivot(pivotColumn: String, values: Seq[Any]): RelationalGroupedDataset
   *
   *  通过 spark.sql.pivotMaxValues 参数这种折叠行数，默认 10000，防止出现 OOM
   *
   *  pivot 逆操作函数 stack 只有 python 版本支持
   *
   */
  @Test
  def pivot(): Unit ={
    val conf = new SparkConf().setAppName("spark-sql").setMaster("local[*]")
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    import spark.implicits._

    try {
      spark.sparkContext.parallelize(Seq(("张三", "数学", 90), ("李四", "数学", 92), ("王五", "数学", 91),
        ("张三", "语文", 90), ("李四", "语文", 92), ("王五", "语文", 91), ("张三", "英语", 90), ("李四", "英语", 92), ("王五", "英语", 91))
      ).toDF("name", "lesson", "score")
        .createTempView("data")

      spark.sql("select * from data").show()
      /**
       * +----+------+-----+
       * |name|lesson|score|
       * +----+------+-----+
       * |  张三|    数学|   90|
       * |  李四|    数学|   92|
       * |  王五|    数学|   91|
       * |  张三|    语文|   90|
       * |  李四|    语文|   92|
       * |  王五|    语文|   91|
       * |  张三|    英语|   90|
       * |  李四|    英语|   92|
       * |  王五|    英语|   91|
       * +----+------+-----+
       */

      // group by 作为 列标，pivot 作为行标
      spark.sql("select * from data").groupBy("name")
        .pivot("lesson")
        .sum("score").show()
      /**
       * +----+---+---+---+
       * |name| 数学| 英语| 语文|
       * +----+---+---+---+
       * |  王五| 91| 91| 91|
       * |  李四| 92| 92| 92|
       * |  张三| 90| 90| 90|是
       * +----+---+---+---+
       */

      // 只对 lesson in ("数学","语文") 的行进行折叠
      spark.sql("select * from data").groupBy("name")
        .pivot("lesson",Seq("数学","语文"))
        .sum("score").show()
      /**
       * +----+---+---+
       * |name| 数学| 语文|
       * +----+---+---+
       * |  王五| 91| 91|
       * |  李四| 92| 92|
       * |  张三| 90| 90|
       * +----+---+---+
       */


      spark.sql("select * from data").groupBy("lesson")
        .pivot("name")
        .sum("score").show()
      /**
       * +------+---+---+---+
       * |lesson| 张三| 李四| 王五|
       * +------+---+---+---+
       * |    英语| 90| 92| 91|
       * |    语文| 90| 92| 91|
       * |    数学| 90| 92| 91|
       * +------+---+---+---+
       */

      // 只对 name in ("张三","李四") 的行进行折叠
      spark.sql("select * from data").groupBy("lesson")
        .pivot("name",Seq("张三","李四"))
        .sum("score").show()
      /**
       * +------+---+---+
       * |lesson| 张三| 李四|
       * +------+---+---+
       * |    英语| 90| 92|
       * |    语文| 90| 92|
       * |    数学| 90| 92|
       * +------+---+---+
       */

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

}
