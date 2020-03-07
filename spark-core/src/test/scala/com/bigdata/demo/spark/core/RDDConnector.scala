package com.bigdata.demo.spark.core

import com.bigdata.demo.spark.util.{CommonSuit, JdbcSuit}
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HBaseAdmin, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.JdbcRDD
import org.junit.Test

class RDDConnector {

  /**
   *  从 JDBC 抓取数据，组织成 rdd 输出
   *  设置上下界（lowerBound，upperBound）目的是为了在个local[n]节点直接分摊抓取任务，放置单个节点抓取数据过多导致内存溢出
   * JdbcRDD[T: ClassTag](
   *  sc: SparkContext,
   *  getConnection: () => Connection,
   *  sql: String,
   *  lowerBound: Long,
   *  upperBound: Long,
   *  numPartitions: Int,
   *  mapRow: (ResultSet) => T = JdbcRDD.resultSetToObjectArray _) 解析每行
   *  extends RDD[T](sc, Nil) with Logging
   */
  @Test
  def jdbc(): Unit ={
    val conf = new SparkConf().setAppName("jdbc").setMaster("local[*]")
    lazy val sc = new SparkContext(conf)

    try {
      new JdbcRDD[(Int, String, Int)](
        sc,
        () => JdbcSuit.getJdbcConnection(),
        "select * from book where id>=? and id<=?",
        1,
        10,
        4,
        r => (r.getInt(1), r.getString(2), r.getInt(3))
      ).foreachPartition(it => println(it.mkString(",")))
      /**
       * (3,Fall of Giants,50)
       * (1,Lie Sporting,30),(2,Pride & Prejudice,70)
       * (8,A2,20),(9,A3,30),(10,a4,40)
       */

      // upsert 写入 mysql
      val tuples = (10 until 20).map(i => (i, s"B${i}", i + 20))

      sc.parallelize(tuples, 4)
        .foreachPartition { it =>
          val longs = JdbcSuit.jdbcBatchExecute(
            """insert into book(id,name,price) values(?,?,?)
              |on duplicate key update name=values(name),price=values(price)""".stripMargin,
            it.map(x => Seq(x._1, x._2, x._3)).toSeq)
          println(longs.mkString(","))
        }

      /**
       * 1,1
       * 1,1,1
       * 1,1,1
       * 1,1
       */
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      sc.stop()
    }
  }


  @Test
  def cassandra(): Unit ={
    val conf = new SparkConf().setAppName("cassandra").setMaster("local[*]")
      .set("spark.cassandra.connection.host", "hadoop01")
      .set("spark.cleaner.ttl", "3600")

    lazy val sc = new SparkContext(conf)

    try {
      // 创建连接时执行初始化脚本(可以不执行)
      CassandraConnector(conf).withSessionDo{session =>
        // 预处理（建表）
        session.execute("create keyspace if not exists test with replication = {'class':'SimpleStrategy','replication_factor':2}")
        // 预处理（自定义 score 类型）
        session.execute(
          """create type if not exists test.score(
            |chinese int,
            |english int,
            |math int,
            |)""".stripMargin)
        // 预处理（建表)）
        session.execute(
          """create table if not exists test.student(
            |id int primary key,
            |name varchar,
            |age int,
            |lessons score
            |)""".stripMargin)
        // 预处理（清空表）
        session.execute("truncate test.student")

        // cqlsh 包含自定义类型的表
        // insert into student(id,name,age,lessons) values(1,'a1',21,{chinese:90,english:91,math:92});
      }

      // 写入
      import com.datastax.spark.connector._
      val tuples = (1 until 10).map(i => (i, s"A${i}", 15 + i, (86 + i, 87 + i, 84 + i)))
      sc.parallelize(tuples)
        .saveToCassandra("test","student",SomeColumns("id","name","age","lessons"))

      // 读取
      implicit val scoreTypeConverter = ScoreTypeConverter
      sc.cassandraTable("test","student")
        .select("id","name","age","lessons")
        .foreach(cr=>println(
          s"""id:${cr.getInt("id")},
             |name:${cr.getString("name")},
             |age:${cr.getInt("age")},
             |lessons:${cr.get[Score]("lessons")}
             |""".stripMargin))
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      sc.stop()
    }
  }

  @Test
  def hbase(): Unit ={
    val conf = new SparkConf().setAppName("hbase").setMaster("local[*]")
    val sc = new SparkContext(conf)

    try {
      val htable = "fruit2"
      val family = "info"
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set("hbase.zookeeper.quorum","hadoop01,hadoop02,hadoop03")
      hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")

      val connection = ConnectionFactory.createConnection(hbaseConf)
      val hAdmin = connection.getAdmin.asInstanceOf[HBaseAdmin]

      // >>>> 1.表不存在就先创建
      if(!hAdmin.isTableAvailable(htable)){
        val htableDesc = new HTableDescriptor(TableName.valueOf(htable))
        htableDesc.addFamily(new HColumnDescriptor(family))
        hAdmin.createTable(htableDesc)
      }

      // >>>>> 2.写入 hbase
      val jobConf = new JobConf(hbaseConf)
      jobConf.setOutputFormat(classOf[TableOutputFormat])
      jobConf.set(TableOutputFormat.OUTPUT_TABLE,htable)

      val tuples = (1 until 10).map(i => (s"${i}", family, s"a${i}", "red"))
      sc.parallelize(tuples).map{x=>
        val put = new Put(Bytes.toBytes(x._1)) // rowkey 必须是 String 类型
        put.addColumn(Bytes.toBytes(x._2),Bytes.toBytes("name"),Bytes.toBytes(x._3))
        put.addColumn(Bytes.toBytes(x._2),Bytes.toBytes("color"),Bytes.toBytes(x._4))
        (new ImmutableBytesWritable,put)
      }.saveAsHadoopDataset(jobConf)

      // >>>> 3.读取 hbase
      hbaseConf.set(TableInputFormat.INPUT_TABLE, htable)

      val hbaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
      hbaseRDD.cache()

      hbaseRDD.foreach {
        case (bytes, result) =>
          val k1 = Bytes.toString(bytes.get())
          val rowkey = Bytes.toString(result.getRow)
          val name = Bytes.toString(result.getValue(family.getBytes, "name".getBytes))
          val color = Bytes.toString(result.getValue(family.getBytes, "color".getBytes))
          println(s"bytes:${k1}, rowkey: ${rowkey}, family: ${family}, name: ${name}, color: ${color}")
      }
      /**
       * bytes:1003, rowkey: 1001, family: info, name: Apple, color: Red
       * bytes:1003, rowkey: 1003, family: info, name: Pineapple, color: Yellow
       */

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      sc.stop()
    }
  }

  /**
   * SparkConf中设置 es 信息
   * saveToEs(resource : scala.Predef.String) : scala.Unit
   * esRDD(resource : scala.Predef.String, query : scala.Predef.String) : org.apache.spark.rdd.RDD[scala.Tuple2[scala.Predef.String,
   *  scala.collection.Map[scala.Predef.String, scala.AnyRef]]]
   */
  @Test
  def elastic1(): Unit ={
    val conf = new SparkConf().setAppName("elastic")
      .setMaster("local[*]")
      .set("es.index.auto.create", "true")
      .set("es.nodes", "hadoop01,hadoop02,hadoop03")
      .set("es.port", "9200")
      .set("es.cluster.name", "es-cluster")
      .set("es.http.timeout","5m")
      .set("es.scroll.size","50")

    val sc = new SparkContext(conf)

    val indexPrefix = "student"

    val tuples = (1 until 10).map(i => Map[String,Any]("type"-> s"${indexPrefix}${i%3}","id"->i, "name" -> s"a${i}", "age" -> (20+i)))

    import org.elasticsearch.spark._

    try{
      // 写入
      sc.parallelize(tuples).saveToEs("{type}")
      // 查询
      val query =
        """{
          | "query":{
          |   "match_all":{}
          | }
          |}""".stripMargin

      sc.esRDD(s"${indexPrefix}1",query).foreach{
        case (_id,json) => println(s"_id: ${_id}, ${json.toString()}")
      }

    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      sc.stop()
    }
  }

  /**
   * 方案 2：单独配置 es 信息
   * saveToEs(cfg : scala.collection.Map[scala.Predef.String, scala.Predef.String]) : scala.Unit
   * esRDD(resource : scala.Predef.String, query : scala.Predef.String, cfg : scala.collection.Map[scala.Predef.String, scala.Predef.String]) :
   *    org.apache.spark.rdd.RDD[scala.Tuple2[scala.Predef.String, scala.collection.Map[scala.Predef.String, scala.AnyRef]]]
   */
  @Test
  def elastic2(): Unit ={
    val conf = new SparkConf().setAppName("elastic").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val indexPrefix = "student"
    var query:String = null

    val tuples = (1 until 10).map(i => Map[String,Any]("type"-> s"${indexPrefix}${i%3}","id"->i, "name" -> s"a${i}", "age" -> (20+i)))

    import org.elasticsearch.spark._

    try{
        val cfg = Map[String, String](
          "es.resource" -> "{type}",
          "es.nodes" -> "hadoop01,hadoop02,hadoop03",
          "es.port" -> "9200",
          "es.index.auto.create" -> "true",
          "es.cluster.name" -> "es-cluster",
          "es.http.timeout" -> "5m",
          "es.scroll.size" -> "50",
          "es.input.json" -> "false", // 写入 map
          "es.write.operation" -> "upsert",
          "es.mapping.exclude" -> "id,type", // 排除json 中指定字段
          "es.mapping.id" -> "id" // 提取 json 中 id 作为 _id
        )
        // 写入
        sc.parallelize(tuples).saveToEs(cfg)
        // 查询
        query =
          """{
            | "query":{
            |   "match_all":{}
            | }
            |}""".stripMargin
        sc.esRDD(s"${indexPrefix}1",query,cfg).foreach{
          case (_id,json) => println(s"_id: ${_id}, ${json.toString()}")
          case _ => null
        }

      // 写入
      sc.parallelize(tuples).map(x=> s"{'name':${ x.get("name") },'age':${x.get("age")}}").saveToEs(cfg)

      // 查询
      query =
        """{
          | "query":{
          |   "match_all":{}
          | }
          |}""".stripMargin
      sc.esRDD(s"${indexPrefix}1",query,cfg).foreach{
        case (_id,json) => println(s"_id: ${_id}, ${json.toString()}")
        case _ => null
      }

    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      sc.stop()
    }
  }

  /**
   * saveToEs,saveJsonToEs 都可以直接将 json 字符串写入 es
   * "es.input.json" -> "true"
   *
   * esJsonRDD 读取 (_id,jstr)
   * esJson 读取 (_id,json)
    */
  @Test
  def elastic3(): Unit ={
    val conf = new SparkConf().setAppName("elastic").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val indexPrefix = "student"
    var query:String = null

    import org.elasticsearch.spark._

    try{
      val cfg = Map[String, String](
        "es.resource" -> "{type}",
        "es.nodes" -> "hadoop01,hadoop02,hadoop03",
        "es.port" -> "9200",
        "es.index.auto.create" -> "true",
        "es.cluster.name" -> "es-cluster",
        "es.http.timeout" -> "5m",
        "es.scroll.size" -> "50",
        "es.input.json" -> "true",  // 写入 json - string
        "es.write.operation" -> "upsert",
//        "es.mapping.exclude" -> "id,type", // 排除json 中指定字段 (插入对象为 json 字符串时，失效)
        "es.mapping.id" -> "id" // 提取 json 中 id 作为 _id
      )

      // 写入
      //      sc.textFile(CommonSuit.getFile("1.json")).saveToEs(cfg) // 直接将字符串写入
      sc.textFile(CommonSuit.getFile("json/1.json")).saveJsonToEs(cfg) // 直接将字符串写入

      // 查询
      query =
        """{
          | "query":{
          |   "match_all":{}
          | }
          |}""".stripMargin

      sc.esJsonRDD(s"${indexPrefix}1",query,cfg).foreach{
        case (_id,json) => println(s"_id: ${_id}, ${json}")
        case _ => null
      }

    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      sc.stop()
    }
  }

  /**
   * saveToEsWithMeta写入 (meta,json) 类型，目前不支持upsert 操作
   *
   */
  @Test
  def elastic4(): Unit ={
    val conf = new SparkConf().setAppName("elastic").setMaster("local[*]")
    val sc = new SparkContext(conf)

    import org.elasticsearch.spark._
    import org.elasticsearch.spark.rdd.Metadata._

    try{
      val cfg = Map[String, String](
        "es.nodes" -> "hadoop01,hadoop02,hadoop03",
        "es.port" -> "9200",
        "es.index.auto.create" -> "true",
        "es.cluster.name" -> "es-cluster",
        "es.http.timeout" -> "5m",
        "es.scroll.size" -> "50",
        "es.input.json" -> "false",  // 写入 json - string
        "es.mapping.id" -> "id" // 提取 json 中 id 作为 _id
      )

      val dataAndMeta = Seq((Map(ID->1),Map("id"->1,"name"->"tom1","age"->21)),
        (Map(ID->2),Map("id"->2,"name"->"tom2","age"->22)),
        (Map(ID->3,VERSION->"23"),Map("id"->3,"name"->"tom3","age"->23))
      )

      // 写入
      sc.parallelize(dataAndMeta).saveToEsWithMeta("student1",cfg) // 直接将字符串写入

      // 查询
      val query =
        """{
          | "query":{
          |   "match_all":{}
          | }
          |}""".stripMargin

      sc.esJsonRDD("student1",query,cfg).foreach{
        case (_id,json) => println(s"_id: ${_id}, ${json}")
        case _ => null
      }

    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      sc.stop()
    }
  }








}
