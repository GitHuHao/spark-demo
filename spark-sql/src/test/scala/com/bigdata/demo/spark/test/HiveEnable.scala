package com.bigdata.demo.spark.test

import com.bigdata.demo.spark.util.CommonSuit
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.Test

class HiveEnable {

  /**
   * spark 连接外置 hive 集群
   * 1：拷贝hive-site.xml 抛配置到 resources 目录
   * 最重要是配置 jdbc 元数据连接信息 和 warehouse 信息
   *
   * spark-shell 连接 hive 集群
   * 1.拷贝 hive0-site.xml 到 $SPARK_HOME/conf 目录
   * spark-shell --master spark://hadoop01:7077,hadoop02:7077 --jars /opt/softwares/spark-2.1.1/lib/mysql-connector-java-5.1.27-bin.jar
   */
  @Test
  def connectHiveClusterByXml(): Unit ={
    val conf = new SparkConf().setAppName("spark-sql").setMaster("local[*]")
    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    try {
      // 查看 hive 集群 db
      spark.sql("show databases").show()
      /**
       * +------------+
       * |databaseName|
       * +------------+
       * |     db_hive|
       * |     default|
       * +------------+
       */

      // session 级别切库
      spark.sql("use default")

      // 建 textfile 表
      spark.sql(
        """CREATE TABLE if not exists `person_text`(
          |  `id` string,
          |  `name` string)
          |PARTITIONED BY (
          |  `month` int)
          |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
          |STORED AS TEXTFILE
          |""".stripMargin)

      // 加载本地数据到 textfile 表
      spark.sql(s"""load data local inpath '${CommonSuit.getFile("hive")}'
           |overwrite into table person_text partition(month=202002)""".stripMargin)

      // 建 parquet 表
      spark.sql(
        """CREATE TABLE if not exists person_parquet (
          |  `id` string,
          |  `name` string)
          |PARTITIONED BY (
          |  `month` int)
          |STORED AS PARQUET
          """.stripMargin
      )

      // 设置动态分区
      spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

      // 从 textfile 表往 parquet 表灌数据
      spark.sql(s"insert into table person_parquet select * from person_text")

      //  查询 parquet
      spark.sql("select * from person_parquet").show()
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

  /**
   * 1.启动 metastore 服务(无需 jdbc 和 hive-site1.xml 支持) （如果无法启动,jps 查看是否有 RunJar 进程存在，杀死即可)）
   * nohup hive --service metastore > log/metastore.log 2>&1 &
   *
   * 编辑 hive-site1.xml 配置（只注册hive.metastore.uris信息），存储到$SPARK_HOME/conf 目录下，然后就可以直接访问 hive
   * spark-shell --master spark://hadoop01:7077,hadoop02:7077
   *
   */
  @Test
  def connectHiveClusterByMetastoreServer(): Unit ={
    val conf = new SparkConf().setAppName("spark-sql").setMaster("local[*]")
    val spark = SparkSession.builder()
      .config(conf)
      .config("hive.metastore.uris","thrift://hadoop01:9083")
      .enableHiveSupport()
      .getOrCreate()

    try {
      // 查看 hive 集群 db
      spark.sql("show databases").show()
      /**
       * +------------+
       * |databaseName|
       * +------------+
       * |     db_hive|
       * |     default|
       * +------------+
       */

      spark.sql("select * from  default.person_parquet").show()
      /**
       * +---+----+------+
       * | id|name| month|
       * +---+----+------+
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


  /**
   * spark 启用内置 hive
   * resources 目录未发现 hive-site1.xml 配置时，使用内置 hive
   * 通过指定 spark.sql.warehouse.dir 可以将内置 hive 存储数据 防止在 hdfs 上
   *
   */
  @Test
  def innerHive(): Unit ={
    val conf = new SparkConf().setAppName("spark-sql").setMaster("local[*]")
    val spark = SparkSession.builder()
      .config(conf)
//      .master("local[*]")
//      .appName("spark-sql")
      .config("spark.sql.warehouse.dir","hdfs://hadoop01:9000/user/spark/warehouse")
      .enableHiveSupport()
      .getOrCreate()
    try {
      spark.sql("show databases").show()

      spark.sql("use default")

      // 建 textfile 表
      spark.sql(
        """CREATE TABLE if not exists `person_text`(
          |  `id` string,
          |  `name` string)
          |PARTITIONED BY (
          |  `month` int)
          |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
          |STORED AS TEXTFILE
          |""".stripMargin)

      // 加载本地数据到 textfile 表
      spark.sql(s"""load data local inpath '${CommonSuit.getFile("hive")}'
                   |overwrite into table person_text partition(month=202002)""".stripMargin)

      // 建 parquet 表
      spark.sql(
        """CREATE TABLE if not exists person_parquet (
          |  `id` string,
          |  `name` string)
          |PARTITIONED BY (
          |  `month` int)
          |STORED AS PARQUET
          """.stripMargin
      )

      // 设置动态分区
      spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

      // 从 textfile 表往 parquet 表灌数据
      spark.sql(s"insert into table person_parquet select * from person_text")

      //  查询 parquet
      spark.sql("select * from person_parquet").show()
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
