package com.bigdata.demo.spark.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.Test

/**
 * 注，开窗函数互斥，不能同时出现
 */
class RankFunc {

  /**
   * 排名连续，相同值，相同名词
   * +---+----+-----+-----+
   * | id|name|score|dense|
   * +---+----+-----+-----+
   * |  1|  a1|   90|    1|
   * |  2|  a2|   89|    2|
   * |  3|  a3|   89|    2|
   * |  4|  a4|   87|    3|
   * |  5|  a5|   86|    4|
   * |  6|  a6|   86|    4|
   * +---+----+-----+-----+
   */
  @Test
  def denseRank(): Unit ={
    val conf = new SparkConf().setAppName("rnak").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._
    try {
      spark.sparkContext.parallelize(Seq(
        (1, "a1", 90), (2, "a2", 89), (3, "a3", 89), (4, "a4", 87), (5, "a5", 86), (6, "a6", 86))
      ).toDF("id", "name", "score").createTempView("student")

      spark.sql(
        """select
          |id,name,score,
          |dense_rank() over(order by score desc) as dense
          |from student
          |order by dense
          |""".stripMargin).show()
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /**
   * 排名不连续，相同排名相同名次
   * +---+----+-----+----+
   * | id|name|score|rank|
   * +---+----+-----+----+
   * |  1|  a1|   90|   1|
   * |  2|  a2|   89|   2|
   * |  3|  a3|   89|   2|
   * |  4|  a4|   87|   4|
   * |  5|  a5|   86|   5|
   * |  6|  a6|   86|   5|
   * +---+----+-----+----+
   */
  @Test
  def rank(): Unit ={
    val conf = new SparkConf().setAppName("rnak").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._
    try {
      spark.sparkContext.parallelize(Seq(
        (1, "a1", 90), (2, "a2", 89), (3, "a3", 89), (4, "a4", 87), (5, "a5", 86), (6, "a6", 86))
      ).toDF("id", "name", "score").createTempView("student")

      spark.sql(
        """select
          |id,name,score,
          |rank() over(order by score desc) as rank
          |from student
          |order by rank
          |""".stripMargin).show()

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /**
   * 排名连续，相同值还是要拍先后
   * +---+----+-----+---+
   * | id|name|score|row|
   * +---+----+-----+---+
   * |  1|  a1|   90|  1|
   * |  2|  a2|   89|  2|
   * |  3|  a3|   89|  3|
   * |  4|  a4|   87|  4|
   * |  5|  a5|   86|  5|
   * |  6|  a6|   86|  6|
   * +---+----+-----+---+
   */
  @Test
  def rowNumber(): Unit ={
    val conf = new SparkConf().setAppName("rnak").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._
    try {
      spark.sparkContext.parallelize(Seq(
        (1, "a1", 90), (2, "a2", 89), (3, "a3", 89), (4, "a4", 87), (5, "a5", 86), (6, "a6", 86))
      ).toDF("id", "name", "score").createTempView("student")

      spark.sql(
        """select
          |id,name,score,
          |row_number() over(order by score desc) as row
          |from student
          |order by row
          |""".stripMargin).show()
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

}
