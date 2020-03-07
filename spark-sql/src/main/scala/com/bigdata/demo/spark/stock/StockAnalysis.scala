package com.bigdata.demo.spark.stock

import com.bigdata.demo.spark.model.{Person, TbDate, TbStock, TbStockDetail}
import com.bigdata.demo.spark.util.CommonSuit
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}


object StockAnalysis{

  def main(args: Array[String]): Unit = {
//    analysisDFWithoutRankOver()
//    analysisDFByRankOver()
    analysisDSByRankOver()
  }

  /**
   * 不借助开窗函数进行统计
   */
  def analysisDFWithoutRankOver(): Unit ={
    val conf = new SparkConf().setAppName("stock").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    try {
      val stock = spark.read
        .csv(CommonSuit.getFile("stock/tbStock.txt"))
        .toDF("ordernumber", "locationid", "dateid")
      stock.cache()
      stock.createTempView("stock")
      stock.show(3)
      /**
       * +------------+----------+---------+
       * | ordernumber|locationid|   dateid|
       * +------------+----------+---------+
       * |BYSL00000893|      ZHAO|2007-8-23|
       * |BYSL00000897|      ZHAO|2007-8-24|
       * |BYSL00000898|      ZHAO|2007-8-25|
       * +------------+----------+---------+
       */

      val detail = spark.read.csv(CommonSuit.getFile("stock/tbStockDetail.txt"))
        .toDF("ordernumber", "rownum", "itemid", "number", "price", "amount")
      detail.cache()
      detail.createTempView("detail")
      detail.show(3)
      /**
       * +------------+------+--------------+------+-----+------+
       * | ordernumber|rownum|        itemid|number|price|amount|
       * +------------+------+--------------+------+-----+------+
       * |BYSL00000893|     0|FS527258160501|    -1|  268|  -268|
       * |BYSL00000893|     1|FS527258169701|     1|  268|   268|
       * |BYSL00000893|     2|FS527230163001|     1|  198|   198|
       * +------------+------+--------------+------+-----+------+
       */

      val date = spark.read.csv(CommonSuit.getFile("stock/tbDate.txt"))
        .toDF("dateid", "years", "theyear", "month", "day", "weekday", "week", "quarter", "period", "halfmonth")
      date.cache()
      date.createTempView("date")
      date.show(3)
      /**
       * +--------+------+-------+-----+---+-------+----+-------+------+---------+
       * |  dateid| years|theyear|month|day|weekday|week|quarter|period|halfmonth|
       * +--------+------+-------+-----+---+-------+----+-------+------+---------+
       * |2003-1-1|200301|   2003|    1|  1|      3|   1|      1|     1|        1|
       * |2003-1-2|200301|   2003|    1|  2|      4|   1|      1|     1|        1|
       * |2003-1-3|200301|   2003|    1|  3|      5|   1|      1|     1|        1|
       * +--------+------+-------+-----+---+-------+----+-------+------+---------+
       */

      // 接字符串 substr
      // 年度订单总金额
      spark.sql(s"""select substr(dateid,0,4) as year,count(distinct stock.ordernumber) as ordernumberCount,sum(amount) as sumAmount from stock
         |left outer join detail on stock.ordernumber=detail.ordernumber
         |group by substr(dateid,0,4)""".stripMargin
      ).show()
      /**
       * +----+----------------+--------------------+
       * |year|ordernumberCount|           sumAmount|
       * +----+----------------+--------------------+
       * |2005|            3828|1.3257564150000002E7|
       * |3274|               1|              1703.0|
       * |2009|            2619|   6323697.189999997|
       * |2006|            3772|1.3680982900000002E7|
       * |2004|            1094|  3268115.4991999995|
       * |2008|            4861|1.4674295300000004E7|
       * |2007|            4885|1.6719354559999995E7|
       * |2010|              94|  210949.65999999995|
       * +----+----------------+--------------------+
       */

      // 接字符串 substr
      // 年度订单总金额
      val yearlyOrderAmount = spark.sql(s"""select substr(dateid,0,4) as year,stock.ordernumber,sum(amount) as sumAmount from stock
                                           |left outer join detail on stock.ordernumber=detail.ordernumber
                                           |group by substr(dateid,0,4),stock.ordernumber""".stripMargin
      )

      yearlyOrderAmount.cache()
      yearlyOrderAmount.createTempView("yearlyOrderAmount")
      yearlyOrderAmount.show(3)
      /**
       * +----+------------+---------+
       * |year| ordernumber|sumAmount|
       * +----+------------+---------+
       * |2007|BYSL00000904|   1272.0|
       * |2007|BYSL00000918|    779.0|
       * |2007|BYSL00000964|   3383.5|
       * +----+------------+---------+
       */

      // 年度最大金额订单
      val yearlyPopularOrder = spark.sql(
        """select temp1.year,temp2.ordernumber,maxAmount from
          |(select year,max(sumAmount) as maxAmount
          |from yearlyOrderAmount group by year) as temp1
          |left outer join yearlyOrderAmount as temp2
          |on temp1.year=temp2.year and temp1.maxAmount=temp2.sumAmount
          |order by temp1.year
          |""".stripMargin)
      yearlyPopularOrder.show()
      /**
       * +----+-------------+------------------+
       * |year|  ordernumber|         maxAmount|
       * +----+-------------+------------------+
       * |2004|HMJSL00001557|23656.799999999974|
       * |2005|HMJSL00003263|38186.399999999994|
       * |2006|HMJSL00006452|           36124.0|
       * |2007|HMJSL00009958|          159126.0|
       * |2008|HMJSL00010598|           55828.0|
       * |2009| TSSL00016101|25813.200000000004|
       * |2010| SSSL00016272|13065.280000000002|
       * |3274| BYSL00013309|            1703.0|
       * +----+-------------+------------------+
       */

      val yearlyItemAmount = spark.sql(s"""select date.theyear,detail.itemid,sum(amount) as sumAmount from stock
                                          |left outer join detail on stock.ordernumber=detail.ordernumber
                                          |left outer join date on stock.dateid=date.dateid
                                          |group by theyear,itemid
                                          |""".stripMargin)
      yearlyItemAmount.cache()
      yearlyItemAmount.createTempView("yearlyItemAmount")
      yearlyItemAmount.show(3)
      /**
       * +-------+--------------+------------------+
       * |theyear|        itemid|         sumAmount|
       * +-------+--------------+------------------+
       * |   2008|CR325403911201|            9419.0|
       * |   2008|01527282681202|            4349.0|
       * |   2008|77127118010502|2931.8999999999996|
       * +-------+--------------+------------------+
       */

      val yearlyPopularItem = spark.sql(
        s"""select * from
           |(
           |    select temp1.theyear,temp2.itemid,maxAmount from
           |    (select theyear,max(sumAmount) as maxAmount from yearlyItemAmount group by theyear) temp1
           |    left outer join yearlyItemAmount as temp2
           |    on temp1.theyear=temp2.theyear and temp1.maxAmount=temp2.sumAmount
           |) temp3 where itemid is not null and theyear is not null
           |order by theyear,maxAmount desc
           |""".stripMargin)
      yearlyPopularItem.show()
      /**
       * +-------+--------------+------------------+
       * |theyear|        itemid|         maxAmount|
       * +-------+--------------+------------------+
       * |   2004|JY424420810101| 53401.76000000003|
       * |   2005|24124118880102| 56627.32999999997|
       * |   2006|JY425468460101|113720.60000000008|
       * |   2007|JY425468460101|           70225.1|
       * |   2008|E2628204040101| 98003.59999999998|
       * |   2009|YL327439080102|           30029.2|
       * |   2010|SQ429425090101|            4494.0|
       * +-------+--------------+------------------+
       */


    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /**
   * 使用 row_number() over(partition by xxx order by yyy) as rank 开窗
   */
  def analysisDFByRankOver(): Unit ={
    val conf = new SparkConf().setAppName("stock").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    try {
      val stock = spark.read
        .csv(CommonSuit.getFile("stock/tbStock.txt"))
        .toDF("ordernumber", "locationid", "dateid")
      stock.cache()
      stock.createTempView("stock")
      stock.show(3)

      val detail = spark.read.csv(CommonSuit.getFile("stock/tbStockDetail.txt"))
        .toDF("ordernumber", "rownum", "itemid", "number", "price", "amount")
      detail.cache()
      detail.createTempView("detail")
      detail.show(3)

      val date = spark.read.csv(CommonSuit.getFile("stock/tbDate.txt"))
        .toDF("dateid", "years", "theyear", "month", "day", "weekday", "week", "quarter", "period", "halfmonth")
      date.cache()
      date.createTempView("date")
      date.show(3)

      // 接字符串 substr
      // 年度订单总金额
      val yearlyOrderAmount = spark.sql(s"""select substr(dateid,0,4) as year,stock.ordernumber,sum(amount) as sumAmount from stock
                                           |left outer join detail on stock.ordernumber=detail.ordernumber
                                           |group by substr(dateid,0,4),stock.ordernumber""".stripMargin
      )

      yearlyOrderAmount.cache()
      yearlyOrderAmount.createTempView("yearlyOrderAmount")
      yearlyOrderAmount.show(3)

      // 年度最大金额订单
      val yearlyPopularOrder = spark.sql(
        """select year,ordernumber,sumAmount from (
          | select year,ordernumber,sumAmount,row_number() over(partition by year order by sumAmount desc) as rank from yearlyOrderAmount
          |) temp where rank=1 and ordernumber is not null and year is not null
          |order by year asc ,sumAmount desc
          |""".stripMargin)

      yearlyPopularOrder.show()

      val yearlyItemAmount = spark.sql(s"""select date.theyear,detail.itemid,sum(amount) as sumAmount from stock
                                          |left outer join detail on stock.ordernumber=detail.ordernumber
                                          |left outer join date on stock.dateid=date.dateid
                                          |group by theyear,itemid
                                          |""".stripMargin)
      yearlyItemAmount.cache()
      yearlyItemAmount.createTempView("yearlyItemAmount")
      yearlyItemAmount.show(3)

      val yearlyPopularItem = spark.sql(
        s"""select theyear,itemid,sumAmount from (
           |  select theyear,itemid,sumAmount,row_number() over(partition by theyear order by sumAmount desc) as rank from yearlyItemAmount
           |) as temp where rank=1 and itemid is not null and theyear is not null
           |order by theyear asc,sumAmount desc
           |""".stripMargin)

      yearlyPopularItem.show()

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /**
   * 使用 row_number() over(partition by xxx order by yyy) as rank 开窗
   */
  def analysisDSByRankOver(): Unit ={
    val conf = new SparkConf().setAppName("stock").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    import spark.implicits._

    try {
      import org.apache.spark.sql.functions._
      val stock:Dataset[TbStock] = spark.read.csv(CommonSuit.getFile("stock/tbStock.txt"))
        .toDF("ordernumber","locationid","dateid")
        .as[TbStock]
      stock.cache()
      stock.show(3)

      val detail = spark.read.csv(CommonSuit.getFile("stock/tbStockDetail.txt"))
        .toDF("ordernumber", "rownum", "itemid", "number", "price", "amount")
        .withColumn("rownum",col("rownum").cast(IntegerType))
        .withColumn("number",col("number").cast(IntegerType))
        .withColumn("price",col("price").cast(DoubleType))
        .withColumn("amount",col("amount").cast(DoubleType))
        .as[TbStockDetail]
      detail.cache()
      detail.show(3)

      val date = spark.read.csv(CommonSuit.getFile("stock/tbDate.txt"))
        .toDF("dateid", "years", "theyear", "month", "day", "weekday", "week", "quarter", "period", "halfmonth")
        .withColumn("years",col("years").cast(IntegerType))
        .withColumn("theyear",col("theyear").cast(IntegerType))
        .withColumn("month",col("month").cast(IntegerType))
        .withColumn("day",col("day").cast(IntegerType))
        .withColumn("weekday",col("weekday").cast(IntegerType))
        .withColumn("week",col("week").cast(IntegerType))
        .withColumn("quarter",col("quarter").cast(IntegerType))
        .withColumn("period",col("period").cast(IntegerType))
        .withColumn("halfmonth",col("halfmonth").cast(IntegerType))
        .as[TbDate]
      date.cache()
      date.show(3)

      stock.join(detail,"ordernumber")
        .select($"dateid".substr(0,4).as("year"),$"ordernumber",$"amount")
        .groupBy($"year")
        .agg(countDistinct($"ordernumber").as("orderCount"),sum($"amount").as("sumAmount"))
        .show()
      /**
       * +----+----------+--------------------+
       * |year|orderCount|           sumAmount|
       * +----+----------+--------------------+
       * |2005|      3828|1.3257564150000002E7|
       * |3274|         1|              1703.0|
       * |2009|      2619|   6323697.189999997|
       * |2006|      3772|1.3680982900000002E7|
       * |2004|      1094|  3268115.4991999995|
       * |2008|      4861|1.4674295300000004E7|
       * |2007|      4885|1.6719354559999995E7|
       * |2010|        94|  210949.65999999995|
       * +----+----------+--------------------+
       */

      val yearlyOrderAmount = stock.join(detail,"ordernumber")
        .select($"dateid".substr(0,4).as("year"),$"ordernumber",$"amount")
        .groupBy($"year",$"ordernumber")
        .agg(sum($"amount").as("amount"))
      yearlyOrderAmount.cache()
      yearlyOrderAmount.show(false)
      /**
       * +----+------------+------------------+
       * |year|ordernumber |amount         |
       * +----+------------+------------------+
       * |2007|BYSL00000904|1272.0            |
       * |2007|BYSL00000918|779.0             |
       * |2007|BYSL00000964|3383.5            |
       * |2008|BYSL00001182|3441.6000000000004|
       * |2008|BYSL00011761|1459.0            |
       * |2008|DGSL00012324|3110.0            |
       * |2009|DGSL00013661|1131.0            |
       * |2007|GCSL00000986|10101.000000000002|
       * |2007|GCSL00001141|5899.299999999999 |
       * |2007|GCSL00001169|2900.2            |
       * |2008|GCSL00001307|0.0               |
       * |2008|GCSL00013493|5461.0            |
       * |2006|GHSL00000722|3787.7400000000007|
       * |2007|GHSL00001179|3504.3            |
       * |2007|GHSL00001244|4655.800000000001 |
       * |2007|GHSL00001351|102.0             |
       * |2007|GHSL00001365|4552.4            |
       * |2007|GHSL00001442|346.8             |
       * |2008|GHSL00001500|1732.0            |
       * |2008|GHSL00001510|2750.0            |
       * +----+------------+------------------+
       */

      yearlyOrderAmount
        .groupBy("year")
        .agg(max($"amount").as("amount"))
        .as("temp1")
        .join(yearlyOrderAmount.as("temp2"),Seq("year","amount"))
        .select("temp1.year","temp2.ordernumber","temp1.amount")
        .orderBy("year")
        .show()
      /**
       * +----+-------------+------------------+
       * |year|  ordernumber|            amount|
       * +----+-------------+------------------+
       * |2004|HMJSL00001557|23656.799999999974|
       * |2005|HMJSL00003263|38186.399999999994|
       * |2006|HMJSL00006452|           36124.0|
       * |2007|HMJSL00009958|          159126.0|
       * |2008|HMJSL00010598|           55828.0|
       * |2009| TSSL00016101|25813.200000000004|
       * |2010| SSSL00016272|13065.280000000002|
       * |3274| BYSL00013309|            1703.0|
       * +----+-------------+------------------+
       */

     val yearlyItemAmount = stock.join(detail,"ordernumber")
        .select($"dateid".substr(0,4).as("year"),$"itemid",$"amount")
        .groupBy($"year",$"itemid")
        .agg(sum($"amount").as("amount"))
      yearlyItemAmount.cache()
      yearlyItemAmount.show(3)
      /**
       * +----+--------------+------------------+
       * |year|        itemid|            amount|
       * +----+--------------+------------------+
       * |2008|CR325403911201|            9419.0|
       * |2008|01527282681202|            4349.0|
       * |2008|77127118010502|2931.8999999999996|
       * +----+--------------+------------------+
       */

      yearlyItemAmount.groupBy($"year")
        .agg(max($"amount").as("amount"))
        .as("temp1")
        .join(yearlyItemAmount,Seq("year","amount"))
        .select($"temp1.year",$"itemid",$"amount")
        .orderBy("year")
        .show()
      /**
       * +----+--------------+------------------+
       * |year|        itemid|            amount|
       * +----+--------------+------------------+
       * |2004|JY424420810101| 53401.76000000003|
       * |2005|24124118880102| 56627.33000000001|
       * |2006|JY425468460101|113720.59999999998|
       * |2007|JY425468460101|           70225.1|
       * |2008|E2628204040101| 98003.59999999998|
       * |2009|YL327439080102|           30029.2|
       * |2010|SQ429425090101|            4494.0|
       * |3274|YA217390232301|             698.0|
       * +----+--------------+------------------+
       */



    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

}