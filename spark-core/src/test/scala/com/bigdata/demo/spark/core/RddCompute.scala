package com.bigdata.demo.spark.core

import Math._

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{HashPartitioner, Partitioner, RangePartitioner, SparkConf, SparkContext}
import org.junit.{After, Before, Test}
import com.bigdata.demo.spark.util.{CommonSuit, JdbcSuit}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.partial.{BoundedDouble, PartialResult}
import parquet.hadoop.codec.SnappyCodec

class RddCompute {

  private var conf: SparkConf= _
  implicit private var sc:SparkContext = _

  @Before
  def before(): Unit ={
    conf = new SparkConf()
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .setAppName("compute")
      .setMaster("local[*]")
    sc = new SparkContext(conf)
  }

  @After
  def after(): Unit ={
    sc.stop()
  }

  /**
   * RDD 一对一转换算子
   * map[U: ClassTag](f: T => U): RDD[U]
   */
  @Test
  def map(): Unit ={
    sc.makeRDD(1 to 3).map(_ + 10).foreach(println(_))
    sc.parallelize(Seq(('A',1),('B',2),('C',1))).map{case (k,v)=> (k.toLower,v*10)}.foreach(println(_)) // case 匹配自动拆包
  }

  /**
   * 直接对 PairRDD 的 value 执行 转换操作
   * mapValues[U](f: V => U): RDD[(K, U)]
   * (a,0)
   * (d,30)
   * (c,20)
   * (b,10
   */
  @Test
  def mapValues(): Unit ={
    val tuples = "abcd".zip(0 until 4)
    sc.parallelize(tuples).mapValues(_*10).foreach(println(_))
  }

  /**
   * 输出元素拆成
   *  flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U]
   */
  @Test
  def flatMap(): Unit ={
    sc.makeRDD(Seq("ab","12")).flatMap(_.toCharArray).foreach(println(_))
  }

  /**
   * 集合类型 value 拆开，与 key 一起发射
   * flatMapValues[U](f: V => TraversableOnce[U]): RDD[(K, U)]
   */
  @Test
  def flatMapValues(): Unit ={
    sc.parallelize(Seq(('A',Seq(1,2,3)),('B',Seq(1,3)),('C',Seq(1))))
      .flatMapValues(_.seq)
      .foreach(println(_))
    /**
     * (B,1)
     * (C,1)
     * (A,1)
     * (B,3)
     * (A,2)
     * (A,3)
     */

    sc.parallelize(Seq(('A',"123"),('B',"123")))
      .flatMapValues(_.toCharArray)
      .foreach(println(_))
    /**
     * (B,1)
     * (B,2)
     * (B,3)
     * (A,1)
     * (A,2)
     * (A,3)
     */

  }

  /**
   * 按指定条件过滤元素
   * filter(f: T => Boolean): RDD[T]
   */
  @Test
  def filter(): Unit ={
    sc.makeRDD(0 to 6).filter(_%2==0).foreach(println(_))
  }

  /** 分区信息
   * def partitions: Array[Partition]
   */
  @Test
  def partitions: Unit ={
    val rdd = sc.parallelize(0 to 16, 4)
    println(rdd.partitions.foreach(x=>println(x.index)))
    /**
     * 0
     * 1
     * 2
     * 3
     * ()
     */
  }

  /**
   * 重新分配分区
   * 1.分区数减小时,无论是否支持 shuffle，都能按预期分区数组织数据
   * 2.分区数增大时，支持 shuffle，才能按预期分区数组织数据，否则无法组织，rdd 维持不变（超过 1000开启 shuffle)）
   * coalesce(numPartitions: Int, shuffle: Boolean = false,
   * partitionCoalescer: Option[PartitionCoalescer] = Option.empty)
   * (implicit ord: Ordering[T] = null)
   * : RDD[T]
   */
  @Test
  def colease(): Unit ={
    val rdd1 = sc.parallelize(0 until 16, 4)
    rdd1.foreachPartition(x=>println(x.mkString(",")))
    /** 4 个分区
     * 12,13,14,15
     * 4,5,6,7
     * 8,9,10,11
     * 0,1,2,3
     */

    val rdd2 = rdd1.coalesce(3, true)
    rdd2.foreachPartition(x=>println(x.mkString(",")))

    /**
     * 运行 shuffle，4 个分区压缩为 3 分区 (按奇偶分区)
     * 1,5,8,11,14
     * 2,6,9,12,15
     * 0,3,4,7,10,13
     */

    val rdd3 = rdd1.coalesce(3, false)
    rdd3.foreachPartition(x=>println(x.mkString(",")))
    /**
     * 不运行 shuffle 4 分区合并为 3分区
     * 0,1,2,3
     * 4,5,6,7
     * 8,9,10,11,12,13,14,15
     */

    val rdd4 = rdd1.coalesce(6, false)
    rdd4.foreachPartition(x=>println(x.mkString(",")))
    /**
     * 不运行 shuffle 4 分区拓展为 6 分区()
     * 0,1,2,3
     * 12,13,14,15
     * 4,5,6,7
     * 8,9,10,11
     */

    val rdd5 = rdd1.coalesce(6, true)
    rdd5.foreachPartition(x=>println(x.mkString(",")))
    /**
     * 运行 shuffle 4 分区拓展为 6 分区()
     * 2,12
     * 1,11
     * 0,7,10
     * 3,4,13
     * 5,8,14
     * 6,9,15
     */
  }

  /**
   * 底层调用的其实就是 coalesce(n,true)
   */
  @Test
  def repartitioner(): Unit ={
    val rdd1 = sc.parallelize(0 until 16 ,4)
    rdd1.foreachPartition(x=>println(x.mkString(",")))
    /**
     * 8,9,10,11
     * 0,1,2,3
     * 12,13,14,15
     * 4,5,6,7
     */

    val rdd2 = rdd1.repartition(2)
    rdd2.foreachPartition(x=>println(x.mkString(",")))
    /**
     * 1,3,5,7,9,11,13,15
     * 0,2,4,6,8,10,12,14
     */

    val rdd3 = rdd1.repartition(6)
    rdd3.foreachPartition(x=>println(x.mkString(",")))
    /**
     * 5,8,14
     * 3,4,13
     * 0,7,10
     * 6,9,15
     * 2,12
     * 1,11
     */
  }

  /**
   * 重新分区同时执行分区内部排序 (先分区，然后在分区内部(基于 key)排序，全局排序使用 sortBy
   * repartitionAndSortWithinPartitions(partitioner: Partitioner): RDD[(K, V)]
   */
  @Test
def repartitionAndSortWithinPartitions(): Unit ={
  val tuples = "deacbf" zip (0 until 6)
  sc.parallelize(tuples,4)
    .repartitionAndSortWithinPartitions(new HashPartitioner(2))
    .foreachPartition(x=>println(x.mkString(",")))
  /**
   * (b,4),(d,0),(f,5)
   * (a,2),(c,3),(e,1)
   */
}

  /**
   * 对 PairRDD 执行分区操作
   * partitionBy(partitioner: Partitioner): RDD[(K, V)]
   */
  @Test
  def partitionBy(): Unit ={
    sc.parallelize(Seq(('a',1),('a',2),('b',1),('b',2),('c',2))).partitionBy(new Partitioner {
      val partitions = Array(0,1)
      override def numPartitions: Int = partitions.length
      override def getPartition(key: Any): Int = partitions(key.asInstanceOf[Char].toInt % 2)
    }).foreachPartition(x=>println(x.mkString(",")))
    /**
     * (b,1),(b,2)
     * (a,1),(a,2),(c,2)
     */

    sc.parallelize(Seq(('a',1),('b',2),('b',20),('c',20)),3)
      .partitionBy(new HashPartitioner(2))
      .foreachPartition(x=> println(x.mkString(",")))
    /**
     * (a,1),(c,
     * (b,2),(b,20)
     */

     val rdd = sc.parallelize(Seq((1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')),3)

     rdd.partitionBy(new RangePartitioner(2, rdd, ascending = false))
      .foreachPartition(x=> println(x.mkString(",")))

    /** 基于 key 区间分区
     * (4,d),(5,e)
     * (1,a),(2,b),(3,c)
     */

    val seq1 = 1.to(10000).map(x=>(100,null)).toSeq
    val seq2 = 1.to(100000).map(x=>(200,null)).toSeq
    val seq3 = (1 to 100).map(x=>(x,null)).toSeq

    val rdd1 = sc.parallelize(seq1.union(seq2).union(seq3))
      .partitionBy(new RandomPartitioner(20))

    rdd1.mapPartitionsWithIndex{case (index,it) => Iterator((index,it.size))}
      .sortByKey(false,1)
      .foreach(println(_))
    /** 重新分区分布
     * (19,7)
     * (18,8)
     * (17,5)
     * (16,6)
     * (15,2)
     * (14,7)
     * (13,6)
     * (12,4)
     * (11,4)
     * (10,2)
     * (9,5)
     * (8,4)
     * (7,3)
     * (6,7)
     * (5,5)
     * (4,9)
     * (3,5)
     * (2,4)
     * (1,3)
     * (0,6)
     */

    val res = rdd1.filter(x => x._1 % 2 == 0)
      .keys
      .sum()
    println(res) // 2850.0
  }

  /** 按比率抽样，返回 RDD
   * sample(
   * withReplacement: Boolean,
   * fraction: Double,
   * seed: Long = Utils.random.nextLong): RDD[T]
   */
  @Test
  def sample(): Unit ={
    sc.makeRDD(0 until 16, 4)
      .sample(false, 0.4, 2)
      .foreachPartition(x=>println(x.mkString(",")))
    /** 无放回抽样 16 * 0.4 = 6.4 四舍五入 6个
     * 12,15
     *
     * 9,11
     * 0,2
     */

    sc.makeRDD(0 until 16, 4)
      .sample(true, 0.4, 2)
      .foreachPartition(x=>println(x.mkString(",")))
    /** 又放回抽样 16 * 0.4 = 6.4 （不重复元素 7 个）
     * 0,1,1
     * 9,9,10,11
     * 12,13
     */
  }

  /**
   * 按个数抽样，返回数组
   * takeSample(
   * withReplacement: Boolean,
   * num: Int,
   * seed: Long = Utils.random.nextLong): Array[T]
   */
  @Test
  def takeSample(): Unit ={
    sc.makeRDD(0 until 16,4)
      .takeSample(false,6,1)
      .foreach(println(_))

    sc.makeRDD(0 until 16,4)
      .takeSample(true,6,1)
      .foreach(println(_))
  }

  /**
   * PairRDDFunc
   * 基于 key 统计个数,返回 map （统计 key 一致个数)）
   * countByKey(): Map[K, Long]
   * (A,2)
   * (B,1)
   * (C,2)
   */
  @Test
  def countByKey(): Unit ={
    sc.parallelize(Seq(('A', 1), ('A', 2), ('B', 1), ('C', 3), ('C', 2)))
      .countByKey()
      .foreach(println(_))
  }

  /**
   * 统计 rdd 元素完全一致的个数
   * countByValue()(implicit ord: Ordering[T] = null): Map[T, Long]
   */
  @Test
  def countByValues(): Unit ={
    sc.parallelize(Seq(('A', 1), ('A', 1), ('B', 1), ('C', 3), ('C', 3)))
      .countByValue()
      .foreach(println(_))
  }

  /**
   * 累计求和
   * reduce(f: (T, T) => T): T
   */
  @Test
  def reduce(): Unit ={
    val sum = sc.parallelize(0 to 3).reduce(_ + _)
    print(sum)
  }

  /**
   * 基于 key 累计求和
   * reduceByKey(func: (V, V) => V): RDD[(K, V)]
   */
  @Test
  def reduceByKey(): Unit ={
   val rdd = sc.textFile("hdfs://hadoop01:9000/apps/mr/wc/in")
      .flatMap(_.split("\\s+"))
      .map((_,1))

    rdd.cache()

    rdd.reduceByKey(_+_)
      .foreachPartition(x=>println(x.mkString(",")))
    /**
     * (f,2),(cc,2),(c,2)
     * (d,4),(s,2),(a,2),(bb,2)
     * (aa,2),(e,2),(ss,1),(b,2)
     */

    rdd.reduceByKey((v1,v2)=>v1 + v2)
      .foreachPartition(x=>println(x.mkString(",")))
    /**
     * (f,2),(cc,2),(c,2)
     * (d,4),(s,2),(a,2),(bb,2)
     * (aa,2),(e,2),(ss,1),(b,2)
     */
  }

  /**
   * 按key聚合
   * groupByKey(numPartitions: Int): RDD[(K, Iterable[V])]
   */
  @Test
  def groupByKey(): Unit ={
    sc.textFile("hdfs://hadoop01:9000/apps/mr/wc/in",4)
      .flatMap(_.split("\\s+"))
      .map((_,1))
      .groupByKey(2)
      .foreachPartition{it=>
        it.foreach(kit=>println(s"key:${kit._1}, data:${kit._2.mkString(",")}"))
      }
    /**
     * key:s, data:1,1
     * key:d, data:1,1,1,1
     * key:e, data:1,1
     * key:aa, data:1,1
     * key:ss, data:1
     * key:a, data:1,1
     * key:b, data:1,1
     * key:c, data:1,1
     * key:f, data:1,1
     * key:bb, data:1,1
     * key:cc, data:1,1
     */
  }

  /**
   * 按指定 方式聚合
   * groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])]
   */
  @Test
  def group(): Unit = {
    sc.textFile("hdfs://hadoop01:9000/apps/mr/wc/in")
      .flatMap(_.split("\\s+"))
      .map((_,1))
      .groupBy(_._1)
      .foreachPartition(it=>it.foreach(x=> println(s"key:${x._1}, data${x._2.mkString(",")}}")))
  }

  /**
   * 两个 RDD 依据 key 执行 group 操作，分别组织为各自迭代器
   * groupWith[W](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))]
   */
  @Test
  def groupWith(): Unit ={
    val rdd1 = sc.parallelize(Seq(('A',1),('A',2),('B',2),('B',3),('C',3)))
    val rdd2 = sc.parallelize(Seq(('A',11),('A',22),('B',22),('B',33),('C',33)))

    rdd1.groupWith(rdd2).foreach{case (key,(it1,it2))=> println(s"${key} ${it1.mkString(",")} ${it2.mkString(",")}")}
  }


  /**
   * 以分区为单位执行转换算子
   * mapPartitions[U: ClassTag](
   * f: Iterator[T] => Iterator[U],
   * preservesPartitioning: Boolean = false): RDD[U]
   * >>>>
   * >>>>
   * >>>>
   * >>>>
   * 10,11,12,13
   * 14,15,16,17
   * 18,19,20,21
   * 22,23,24,25
   */
  @Test
  def mapPartitions(): Unit ={
    sc.parallelize(0 until 16, 4)
      .mapPartitions{it=>
        println(">>>>")
        it.map(_+10)
      }.foreachPartition(it=>println(it.mkString(",")))

    val tuples = "abcdef" zip Seq(1, 2, 2, 4, 2, 6)

    sc.parallelize(tuples,3).mapPartitions{it=>
      var seq = List[Char]()
      while(it.hasNext){
        val next = it.next()
        next match {
          case (x,2) => seq = x :: seq
          case _ =>
        }
      }
      seq.iterator
    }.foreachPartition(x=>println(x.mkString(",")))
    /**
     * e
     * c
     * b
     */
  }

  /**
   * 以 partition 为单位筛选，并附带分区数
   * mapPartitionsWithIndex[U: ClassTag](
   * f: (Int, Iterator[T]) => Iterator[U],
   * preservesPartitioning: Boolean = false): RDD[U]
   */
  @Test
  def mapPartitionWithIndex(): Unit ={
    sc.parallelize(0 until 16,4)
      .mapPartitionsWithIndex{(idx,it)=>
        it.filter(_%2==0).map((idx,_))
      }.foreachPartition(x=>println(x.mkString(",")))
    /**
     * (0,0),(0,2)
     * (3,12),(3,14)
     * (1,4),(1,6)
     * (2,8),(2,10)
     */
  }

  /**
   * rdd 合并，通常需要压缩分区
   * union(other: RDD[T]): RDD[T]
   */
  @Test
  def union(): Unit ={
    val rdd1 = sc.parallelize(0 until 4, 2)
    val rdd2 = sc.parallelize(4 until 8, 2)
    rdd1.union(rdd2).coalesce(2).foreachPartition(x=>println(x.mkString(",")))
    /**
     * 4,5,6,7
     * 0,1,2,3
     */

    val rdd3 = sc.parallelize(Seq(('a',1),('b',2),('c',3)))
    val rdd4 = sc.parallelize(Seq(('b',22),('c',33),('d',44)))
    rdd3.union(rdd4).coalesce(2).foreachPartition(x=>println(x.mkString(",")))

    /**
     * (b,22),(c,33),(d,44)
     * (a,1),(b,2),(c,3)
     */
  }

  /**
   * rdd 交集
   * intersection(other: RDD[T]): RDD[T]
   */
  @Test
  def intersection(): Unit ={
    val rdd1 = sc.parallelize(0 to 4)
    val rdd2 = sc.parallelize(1 to 5)
    rdd1.intersection(rdd2).foreach(println(_))
  }

  /**
   * 差集
   */
  @Test
  def substract(): Unit ={
    val rdd1 = sc.parallelize(0 to 4)
    val rdd2 = sc.parallelize(1 to 5)
    rdd1.subtract(rdd2).foreach(println(_))
  }

  /**
   * 全局去重
   * distinct(): RDD[T]
   *  map(x => (x, null)).reduceByKey((x, y) => x, numPartitions).map(_._1)
   */
  @Test
  def distinct(): Unit ={
    sc.parallelize(Seq(1,2,3,2,1,4),4).distinct().foreach(println(_))
    /**
     * 1
     * 2
     * 4
     * 3
     */
    sc.parallelize(Seq(1,1,1,1,1,1,1,2,1,2,1,2,1),3)
      .distinct(2)(new Ordering[Int]{
        override def compare(x: Int, y: Int): Int = y-x
      }).foreach(println(_))

    /**
     * 去重同时，进行排序
     */

  }

  /**
   * 遍历元素
   * foreach(f: T => Unit): Unit
   */
  @Test
  def foreach(): Unit ={
    val rdd = sc.parallelize(0 to 3,3)
    rdd.foreachPartition(x=>println(s">>${x.mkString(",")}"))
    rdd.foreach(println(_))
    /**
     * >>2,3
     * >>1
     * >>0
     * 1
     * 2
     * 3
     * 0
     */

    val sum = sc.longAccumulator("sum") // 累加器执行累计统计
    sc.parallelize(0 until 3)
      .foreach(sum.add(_))
    println(sum.value)

    sc.parallelize(Seq(('a',1),('b',2))).foreach{case (k,v)=> println(s"key: ${k}, value: ${v}")}
    /**
     * key: b, value: 2
     * key: a, value: 1
     */
  }

  @Test
  def foreachAsync(): Unit ={
    sc.textFile("hdfs://hadoop01:9000/apps/mr/wc/in")
      .flatMap(_.split(","))
      .map((_,1))
      .reduceByKey(_+_)
      .foreachAsync(x=>print(s"async: ${x}")) // 异步输出，先执行下面 async~
    println("async~")

    sc.textFile("hdfs://hadoop01:9000/apps/mr/wc/in")
      .flatMap(_.split(","))
      .map((_,1))
      .reduceByKey(_+_)
      .foreach(x=>print(s"sync: ${x}")) // 同步阻塞
    println("sync~")
  }

  /**
   * 以分区为单位执行算子
   * foreachPartition(f: Iterator[T] => Unit): Unit
   */
  def foreachePartition(): Unit ={
    sc.parallelize(0 until 16, 4)
      .mapPartitions{it=>
        println(">>>>")
        it.map(_+10)
      }.foreachPartition(it=>println(it.mkString(",")))
  }

  /**
   * 1.rdd 收集为数组
   * collect(): Array[T]
   * 2.rdd 执行偏函数收集
   * collect[U: ClassTag](f: PartialFunction[T, U]): RDD[U]
   */
  @Test
  def collect(): Unit ={
    val rdd:RDD[Any] = sc.parallelize(Seq(1,2,'3','4'),2)
    rdd.collect().foreach(println(_))

    rdd.collect {
      case x: Char => x.asInstanceOf[Int]
      case x: Int => x
      case _ => -1
    }.filter(_!= -1).collect().foreach(println(_))
  }

  /**
   * rdd kv 结构收集为 map
   * collectAsMap(): Map[K, V]
   */
  @Test
  def collectAsMap(): Unit ={
    val res = sc.parallelize(Seq(('A', 1), ('B', 1), (('A', 2))))
      .collectAsMap()
    println(res)
  }

  /**
   * PairRDD 中查找 key 对应全部 value ，组成集合返回
   * lookup(key: K): Seq[V]
   */
  @Test
  def lookup(): Unit ={
    val tuples = "abace".zip(0 until 10)
    println(tuples) // zip 就短原则
    val ints = sc.parallelize(tuples).lookup('a')
    println(ints.mkString(","))
  }


  /**
   * 计数
   * 返回 rdd，总元素个数
   * count(): Long
   */
  @Test
  def rddCount(): Unit ={
    val num = sc.parallelize(0 until 3, 3).count()
    println(num)
  }

  /**
   * 求和
   * 默认返回都是 double 类型
   * DoubleRDDFunctions
   */
  @Test
  def rddSum(): Unit ={
    val res = sc.parallelize(0 until 3).sum()
    println(res) // 3.0
  }

  /**
   * 近似计算，超时前，返回机损结果
   * sumApprox(
   * timeout: Long,
   * confidence: Double = 0.95): PartialResult[BoundedDouble]
   */
  @Test
  def rddSumApprox(): Unit ={
    var task: PartialResult[BoundedDouble] = null
    val rdd1 = sc.parallelize(0 until 999999999, 8)
    task = rdd1.sumApprox(1)

    task.onComplete(x=>println(s"high:${x.high}, low:${x.low}"))

    task.onFail{e=>
      println(s"final value: ${task.getFinalValue()}")
      println(">>>>")
      e.printStackTrace()
    }
    Thread.sleep(5000)
  }


  /**
   * 均值
   */
  @Test
  def rddMean(): Unit ={
    val res = sc.parallelize(0 until 3).mean()
    println(res) // 3.0
  }

  /**
   * 最大值
   */
  @Test
  def rddMax(): Unit ={
    val res = sc.parallelize(0 until 3).max()
    println(res) // 3.0
  }

  /**
   * 最小值
   */
  @Test
  def rddMin(): Unit ={
    val res = sc.parallelize(0 until 3).min()
    println(res) // 0.0
  }

  /**
   * 方差
   */
  @Test
  def rddVariance(): Unit ={
    val res = sc.parallelize(0 until 3).variance()
    println(res) // 0.6666666666666666
  }

  /**
   * 样本方差
   */
  @Test
  def rddSampleVariance(): Unit ={
    val res = sc.parallelize(0 until 3).sampleVariance()
    println(res) // 1.0
  }

  /**
   * 标准差
   */
  @Test
  def rddStdev(): Unit ={
    val res = sc.parallelize(0 until 3).stdev()
    println(res) // 1.0
  }

  /**
   * 样本标准差
   */
  @Test
  def rddSampleStdev(): Unit ={
    val res = sc.parallelize(0 until 3).sampleStdev()
    println(res) // 1.0
  }

  /**c
   * 返回 rdd 首个分区，第一个元素
   * first(): T
   */
  @Test
  def first(): Unit ={
    val elem1 = sc.parallelize(0 to 3).first()
    println(elem1)

    val elem2 = sc.parallelize(0 to 3,3).first()
    println(elem2)
  }

  /**
   * 将前 num 个元素取出，返回数组（索引超限、或负值，不报错）不排序
   * take(num: Int): Array[T]
   */
  @Test
  def take(): Unit ={
    val ints = sc.parallelize(0 until 5, 2).take(8)
    println(ints.mkString(","))
  }

  /**
   * 先排序，然后去前 n个元素输出，（提供隐式参数接收比较函数）
   * takeOrdered(num: Int)(implicit ord: Ordering[T])
   */
  @Test
  def takeOrderd(): Unit ={
    val rdd = sc.parallelize(Seq(4,3,5,6,2,7,5,1),4)
    rdd.take(3).foreach(println(_))
    /**
     * 4
     * 3
     * 5
     */

    rdd.takeOrdered(3).foreach(println(_))
    /**
     * 1
     * 2
     * 3
     */

    sc.parallelize("bdcefa".zip(0 until 6)).takeOrdered(3)(new Ordering[(Char,Int)] {
      override def compare(x: (Char, Int), y: (Char, Int)): Int = x._2.compareTo(y._2)
    }).foreach(println(_))

    /**
     * (b,0)
     * (d,1)
     * (c,2)
     */

  }

  /**
   * 自定义聚合算子
   * combineByKey[C](
   *  createCombiner: V => C, 初始化元素
   *  mergeValue: (C, V) => C, 分区内聚合
   *  mergeCombiners: (C, C) => C): RDD[(K, C)] 分区间合并
   */
  @Test
  def combineByKey(): Unit ={
    sc.parallelize(Seq(('a',10),('a',12),('b',14),('c',10),('c',14)))
      .combineByKey(
        (v)=>(v,1),
        (acc:(Int,Int),v)=> (acc._1+v,acc._2+1),
        (acc1:(Int,Int),acc2:(Int,Int)) => (acc1._1 + acc2._1,acc1._2 + acc2._2)
      ).map(x=>(x._1,x._2._1/(x._2._2*1.0)))
      .foreach(println(_))
    /**
     * (c,12.0)
     * (a,11.0)
     * (b,14.0)
     */

    sc.textFile("hdfs://hadoop01:9000/apps/mr/wc/in",4)
      .flatMap((_.split("\\s+")))
      .map((_,1))
      .combineByKey(
        (v)=>v,
        (sum:Int,v)=> sum + v,
        (sum1:Int,sum2:Int) => sum1 + sum2
      ).foreach(println(_))

    /** combineByKey 执行 reduceByKey
     * (e,2)
     * (c,2)
     * (s,2)
     * (a,2)
     * (d,4)
     * (bb,2)
     * (aa,2)
     * (ss,1)
     * (b,2)
     * (f,2)
     * (cc,2)
     */

    sc.makeRDD(Seq(('a',1),('a',2),('a',3),('b',10),('b',20),('c',20)))
      .combineByKey(
        (v)=>(v,v,v,1,v*1.0),
        (agg:(Int,Int,Int,Int,Double),v)=> (max(agg._1,v),min(agg._2,v),agg._3+v,agg._4+1,v*1.0),
        (agg1:(Int,Int,Int,Int,Double),agg2:(Int,Int,Int,Int,Double)) => (
          max(agg1._1,agg2._1),
          min(agg1._2,agg2._2),
          agg1._3 + agg2._3,
          agg1._4 + agg2._4,
          (agg1._3 + agg2._3)/((agg1._4 + agg2._4)*1.0)
        )
      ).foreach(x=>println(s"key:${x._1} max:${x._2._1} min:${x._2._2} sum:${x._2._3} count: ${x._2._4} avg:${x._2._5}"))
    /**
     * key:b max:20 min:10 sum:30 count: 2 avg:15.0
     * key:c max:20 min:20 sum:20 count: 1 avg:20.0
     * key:a max:3 min:1 sum:6 count: 3 avg:2.0
     */

  }

  /**
   * 基于 key 聚合对象
   * combineByKeyWithClassTag[C](
   * createCombiner: V => C,
   * mergeValue: (C, V) => C,
   * mergeCombiners: (C, C) => C)(implicit ct: ClassTag[C]): RDD[(K, C)]
   */
  @Test
  def combingByKeyWithClassTag(): Unit ={
    sc.parallelize(Seq((1,Person("tom",21)),(1,Person("tom",22)),(2,Person("tom",23)),(2,Person("jack",20)),(2,Person("lucy",22)),(3,Person("lucy",25))))
      .combineByKeyWithClassTag(
        (v)=>(List(v),1),
        (acc:(List[Person],Int),v) => (v::acc._1,1),
        (acc1:(List[Person],Int),acc2:(List[Person],Int)) => (acc1._1:::acc2._1,acc1._2 + acc2._2)
      ).foreachPartition(x=>println(x.mkString(",")))
    /**
     * (3,(List(Person(lucy,25)),1))
     * (1,(List(Person(tom,21), Person(tom,22)),2))
     * (2,(List(Person(tom,23), Person(jack,20), Person(lucy,22)),3))
     */
  }

  /**
   * 聚合操作
   * aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,
   * combOp: (U, U) => U): RDD[(K, U)]
   */
  @Test
  def aggregateByKey(): Unit ={
    sc.parallelize(Seq((1,1),(1,2),(1,3),(2,2),(2,3)))
      .aggregateByKey(0)(_+_,_+_)
      .foreach(println(_))
    /**
     * (2,5)
     * (1,6)
     */

    sc.parallelize(Seq((1,1),(1,2),(1,3),(2,2),(2,3),(3,3)))
      .aggregateByKey(0)(max(_,_),max(_,_))
      .foreach(println(_))

    /**
     * (2,3)
     * (3,3)
     * (1,3)
     */

    sc.parallelize(Seq((1,1),(1,2),(1,3),(2,2),(2,3),(3,3)))
      .aggregateByKey(0)(min(_,_),min(_,_))
      .foreach(println(_))

    /**
     * (2,0)
     * (1,0)
     * (3,0)
     */
  }

  /**
   * 累计聚合
   *  aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U
   *  U: ClassTag 最终输出值
   *  zeroValue 初始值
   *  seqOp 分区内部聚合函数
   *  combOp 分区间合并函数
   */
  @Test
  def aggregate(): Unit ={
    val sumRes = sc.parallelize(0 until 4)
      .aggregate(0)(_ + _, _ + _)
    println(sumRes)

    val maxRes = sc.parallelize(0 until 4)
      .aggregate(0)(max(_,_), max(_,_))
    println(maxRes)

    val minRes = sc.parallelize(0 until 4)
      .aggregate(0)(min(_,_), min(_,_))
    println(minRes)
  }

  /**
   * aggregate 的 seqOp 与 combOp 一致时使用 fold
   */
  @Test
  def fold(): Unit ={
    val sum = sc.parallelize(0 until 4).fold(0)(_ + _)
    println(sum)
  }

  /**
   * key 相同的折叠累计求和
   */
  @Test
  def foldByKey(): Unit ={
    sc.parallelize(Seq(('A',1),('A',2),('A',3),('b',2),('B',4)))
      .foldByKey(0)(_+_)
      .foreach(println(_))
    /**
     * (B,4)
     * (A,6)
     * (b,2)
     */
  }



  /**
   * 对 PairRDD 按 Keysort进行全局排序，并可重新指定分区数 （分区内部排序使用 repartitionAndSortWithPartitions）
   * sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.length)
   * : RDD[(K, V)]
   */
  @Test
  def sortByKey(): Unit ={
    // 分区内基于 key 排序
    sc.parallelize(Seq(('d',4),('a',1),('a',2),('b',2)),2)
      .sortByKey(ascending = false) // 降序
      .foreachPartition(x=>println(x.mkString(",")))
    /**
     * (d,4),(b,2)
     * (a,1),(a,2)
     */

    // 若想设置为全局排序，设置为一个分区即可
    sc.parallelize(Seq(('d',4),('a',1),('a',2),('b',2)),1)
      .sortByKey(ascending = false)
      .foreachPartition(x=>println(x.mkString(",")))
    /**
     * (d,4),(b,2),(a,1),(a,2)
     */

    sc.parallelize(Seq(('A',1),('D',4),('C',3),('B',2)),4)
      .sortByKey(false,2)
      .foreachPartition(x=>println(x.mkString(",")))
    /**
     * (B,2),(A,1)
     * (D,4),(C,3)
     */
  }

  /**
   * 按给定函数执行排序操作
   * sortBy[K](
   * f: (T) => K,
   * ascending: Boolean = true,
   * numPartitions: Int = this.partitions.length)
   * (implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T]
   *
   * f: (T) => K 输出值作为比较字段
   */
  @Test
  def sortBy(): Unit ={
    sc.parallelize(0 to 16,4)
      .sortBy(x=>x,false) // 分区内部按数值降序排序
      .foreachPartition(x=>println(x.mkString(",")))
    /**
     * 8,7,6,5
     * 4,3,2,1,0
     * 12,11,10,9
     * 16,15,14,13
     */
    
    sc.parallelize(0 to 16,4)
      .sortBy(x=>x%2,true) // 分区内部按就排序（奇前偶后)）
      .foreachPartition(x=>println(x.mkString(",")))
    /**
     * 0,2,4,6,8,10,12,14,16
     * 1,3,5,7,9,11,13,15
     */
  }

  /**
   * 连接操作，join底层调用的其实是 cogroup
   * join[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, W))]
   *
   * leftOuterJoin[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, Option[W]))]
   *
   * rightOuterJoin[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (Option[V], W))]
   *
   * fullOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], Option[W]))]
   *
   */
  @Test
  def join(): Unit ={
    val rdd1 = sc.parallelize(Seq(('A', 1), ('B', 2), ('C', 3)))
    val rdd2 = sc.parallelize(Seq(('B', 20), ('C', 30),('D',40)))
    rdd1.join(rdd2, 2)
      .foreachPartition(x=>println(s"~~~${x.mkString(",")}"))

    rdd1.cogroup(rdd2).flatMapValues{it=>
      for(i<- it._1;j<-it._2) yield (i,j)
    }.foreach(x=>println(s"~~${x}"))
    /**
     * 默认是 inner join
     * ~~~(C,(3,30))
     * ~~~(B,(2,20))
     * ~~(C,(3,30))
     * ~~(B,(2,20))
     */

    rdd1.leftOuterJoin(rdd2)
      .foreachPartition(_.foreach(x=>println(s"---${x._1},${x._2._1} ${x._2._2.getOrElse(null)}")))

    rdd1.cogroup(rdd2).flatMapValues{it=>
      if(it._2.isEmpty){
        it._1.iterator.map((_,None))
      }else{
        for(i<-it._1;j<-it._2) yield (i,Some(j))
      }
    }.foreach(x=>println(s"--- ${x}"))
    /**
     * ---B,2 20
     * ---A,1 null
     * ---C,3 30
     * --- (A,(1,None))
     * --- (C,(3,Some(30)))
     * --- (B,(2,Some(20)))
     */

    rdd1.rightOuterJoin(rdd2)
      .foreachPartition(_.foreach(x=>println(s"<<<${x._1} ${x._2._1.getOrElse(null)} ${x._2._2}")))

    rdd1.cogroup(rdd2).flatMapValues{it=>
      if(it._1.isEmpty){
        it._2.iterator.map((None,_))
      }else{
        for(i<-it._1;j<-it._2) yield (Some(i),j)
      }
    }.foreach(x=>println(s"<< ${x}"))
    /**
     * <<<D null 40
     * <<<C 3 30
     * <<<B 2 20
     * << (D,(None,40))
     * << (B,(Some(2),20))
     * << (C,(Some(3),30))
     */

    rdd1.fullOuterJoin(rdd2)
      .foreachPartition(_.foreach(x=> println(s">>>${x._1} ${x._2._1.getOrElse(null)} ${x._2._2.getOrElse(null)}")))

    rdd1.cogroup(rdd2).flatMapValues{it=>
      if(it._1.isEmpty){
        it._2.iterator.map((None,_))
      }else if(it._2.isEmpty){
        it._1.iterator.map((_,None))
      }else{
        for(i<-it._1;j<-it._2) yield (Some(i),Some(j))
      }
    }.foreach(x=>println(s">> ${x}"))
    /**
     * >>>A 1 null
     * >>>B 2 20
     * >>>D null 40
     * >>>C 3 30
     * >> (D,(None,40))
     * >> (A,(1,None))
     * >> (C,(Some(3),Some(30)))
     * >> (B,(Some(2),Some(20)))
     */
  }

  /**
   * rdd 连接环后，分别按 key 收集各自元素组成迭代器
   * cogroup[W](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))]
   */
  @Test
  def cogroup(): Unit ={
    val rdd1 = sc.parallelize(Seq(('A', 1),('A', 1), ('B', 2), ('C', 3)))
    val rdd2 = sc.parallelize(Seq(('B', 20),('B', 20), ('C', 30),('D',40)))

    rdd1.cogroup(rdd2).foreach(it=>println(s">>> ${it._1} ${it._2._1.mkString(",")} ${it._2._2.mkString(",")}"))

    // inner join
    rdd1.cogroup(rdd2).flatMapValues{it=>
      for(l<- it._1;r<-it._2) yield (l,r)
    }.foreach(x=>println(s"--- ${x}"))


    rdd1.cogroup(rdd2).flatMapValues{it=>
      if(it._2.isEmpty){
        it._1.iterator.map((_,None))
      }else{
        for(i<-it._1;j<-it._2) yield (i,j)
      }
    }.foreach(x=>println(s"<<<${x}"))

  }

  /**
   * 提取 PairRDD 的key 组成新 RDD
   * def keys: RDD[K] = self.map(_._1)
   */
  @Test
  def keys(): Unit ={
    sc.parallelize(Seq(('A', 1), ('B', 1))).keys.foreach(println(_))
  }

  /**
   * 提取 PairRDD 的value 组成新 RDD
   * def values: RDD[V] = self.map(_._2)
   */
  @Test
  def values(): Unit ={
    sc.parallelize(Seq(('A', 1), ('B', 1))).values.foreach(println(_))
  }

  /**
   * 笛卡尔集合
   * cartesian[U: ClassTag](other: RDD[U]): RDD[(T, U)]
   */
  @Test
  def cartesian(): Unit ={
    val rdd1 = sc.parallelize(Seq(('A', 1),('B', 2)))
    val rdd2 = sc.parallelize(Seq(('C',3),('D',4)))

    rdd1.cartesian(rdd2).foreach(println(_))
    /**
     * ((A,1),(C,3))
     * ((A,1),(D,4))
     * ((B,2),(C,3))
     * ((B,2),(D,4))
     */
  }

  /**
   * pipe(command: String): RDD[String]
   *
   * echo-pipe.sh
   * --------------------
   * #!/usr/bin/env bash
   *
   * echo 'start'
   *
   * while read host; do
   *  # read host 读取分区信息
   *    echo `ping -c 1 $host`
   *  # 初始化转换后内容
   * done
   *
   * exit 0
   * --------------------
   *
   * add_pipe.py
   * --------------------
   * #!/usr/bin/python
   *
   * import sys
   * from builtins import int
   *
   * for line in sys.stdin:
   * num = int(line)
   * print(num * 100)
   * --------------------
   */
  @Test
  def pipe(): Unit ={
    sc.parallelize(Seq("www.baidu.com","www.google.com"),2)
      .pipe(CommonSuit.getFile("pipe/echo-pipe.sh")).foreachPartition(x=>println(x.mkString(",")))

    sc.parallelize(0 until 3,2)
      .pipe(CommonSuit.getFile("pipe/add_pipe.py")).foreachPartition(x=>println(x.mkString(",")))
  }

  /**
   * 分区收集成数组，组成新 RDD
   * glom(): RDD[Array[T]]
   */
  @Test
  def glom(): Unit ={
    sc.parallelize(0 until 16, 4)
      .glom().foreach(x=>println(x.mkString(",")))
    /**
     * 0,1,2,3
     * 8,9,10,11
     * 4,5,6,7
     * 12,13,14,15
     */
  }

  /**
   * 存储 MapRDD （底层调用的是saveAsHadoopFile）
   * 按行读取文件
   * textFile(
   *  path: String,
   *  minPartitions: Int = defaultMinPartitions): RDD[String]
   *
   * 输出 textFile （一个分区，一个文件，空分区，会形成空文件)）
   * saveAsTextFile(path: String):
   */
  @Test
  def textFile(): Unit ={
    val in = "hdfs://hadoop01:9000/apps/mr/wc/in"
    val out = "hdfs://hadoop01:9000/apps/mr/wc/out1"
    CommonSuit.deleteHdfsDir(out)
    sc.textFile(in)
      .flatMap(_.split("\\s+"))
      .map((_,1))
      .reduceByKey(_+_)
      .saveAsTextFile(out)
  }

  /**
   * 存储 PairRDD （底层调用的是saveAsHadoopFile）
   *  sequence 不声明压缩编码，使用默认压缩
   * rdd 序列化存储到 hdfs
   * saveAsSequenceFile(
   *  path: String,
   *  codec: Option[Class[_ <: CompressionCodec]] = None): Unit
   *
   * 读取 hdfs 数据，然后还原为 RDD（需要设置隐式参数，声明 RDD类型）
   * sequenceFile[K, V]
   *  (path: String, minPartitions: Int = defaultMinPartitions)
   *  (implicit km: ClassTag[K], vm: ClassTag[V],
   *  kcf: () => WritableConverter[K], vcf: () => WritableConverter[V]): RDD[(K, V)]
   *
   */
  @Test
  def sequenceFile(): Unit ={
    val in = "hdfs://hadoop01:9000/apps/mr/wc/in"
    val out = "hdfs://hadoop01:9000/apps/mr/wc/out2"

    CommonSuit.deleteHdfsDir(out)

    sc.textFile(in)
      .flatMap(_.split("\\s+"))
      .map((_,1))
      .reduceByKey(_+_)
      .saveAsSequenceFile(out)

    sc.sequenceFile[String,Int](out).foreachPartition(x=>println(x.mkString(","))) // 声明隐式参数 1

    val rdd:RDD[(String,Int)] = sc.sequenceFile[String,Int](out) // 声明隐式参数 2
    rdd.foreachPartition(x=>println(x.mkString(",")))

    // parquet snappy 压缩
    sc.textFile(in)
      .flatMap(_.split("\\s+"))
      .map((_,1))
      .reduceByKey(_+_)
      .saveAsSequenceFile(out,Some(classOf[SnappyCodec]))

    sc.sequenceFile[String,Int](out).foreachPartition(x=>println(x.mkString(","))) // 声明隐式参数 1

    val rdd2:RDD[(String,Int)] = sc.sequenceFile[String,Int](out) // 声明隐式参数 2
    rdd2.foreachPartition(x=>println(x.mkString(",")))

  }

  /**
   * 存储对象(底层调用的是 saveSequenceFile)
   *
   * saveAsObjectFile(path: String): Unit
   *
   * objectFile[T: ClassTag](
   *  path: String,
   *  minPartitions: Int = defaultMinPartitions): RDD[T]
   *
   */
  @Test
  def objectFile(): Unit ={
    val out = "hdfs://hadoop01:9000/apps/mr/wc/out3"
    CommonSuit.deleteHdfsDir(out)

    sc.parallelize(Seq(Person("AA",12),Person("AB",13)))
        .saveAsObjectFile(out)

    sc.objectFile[Person](out).foreach(println(_))
  }

  /**
   * 输出到 hdfs （需要设置隐式参数 输出 key:Text:IntWritable,value:TextOutputFormat[Text,IntWritable],输出格式）
   * saveAsNewAPIHadoopFile[F <: NewOutputFormat[K, V]](
   *  path: String)(implicit fm: ClassTag[F]): Unit
   *
   *  读取 hdfs文件(需要设置隐式参数 输入 key:LongWritable, value:Text, TextInputFormat) 按行读取，key 为字节数，value 为行内容
   * newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]]
   *  (path: String)
   *  (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)]
   *
   */
  @Test
  def hadoopFile(): Unit ={
    val in = "hdfs://hadoop01:9000/apps/mr/wc/in"
    val out = "hdfs://hadoop01:9000/apps/mr/wc/out4"
    CommonSuit.deleteHdfsDir(out)

    sc.textFile(in)
      .flatMap(_.split("\\s+"))
      .map((_,1))
      .reduceByKey(_+_)
      .saveAsNewAPIHadoopFile(out,classOf[Text],classOf[IntWritable],classOf[TextOutputFormat[Text,IntWritable]])

    sc.newAPIHadoopFile[LongWritable, Text, TextInputFormat](out).foreach(x=>println(x))
  }

  @Test
  def jsonFile(): Unit ={
    val out = "hdfs://hadoop01:9000/apps/mr/wc/out5"
    CommonSuit.deleteHdfsDir(out)

    // 为避免 json4s 序列化问题，此处将 对象与 json 直接序列化操作，闭包在半生类对象中吗，然后对象自身实现序列化
    sc.parallelize(Seq(Person("tom",21),Person("jack",22)))
        .map(Person.dumps(_))
        .saveAsTextFile(out)

    sc.textFile(out)
      .map(Person.loads(_))
      .foreach(println(_))
  }







































}
