package com.bigdata.demo.spark.graph

import org.junit.Test
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

class GraphInfo extends Serializable {

  @Test
  def demo(): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("demo")
    val sc = new SparkContext(conf)
    try {
      // 创建顶点RDD 二元祖结构，第一个元素是顶点id，第二个元素为其他集合类型，此处为另一个二元组，分别包含姓名，职业等属性
      val users: RDD[(VertexId, (String, String))] = sc.parallelize(Seq((3l, ("rxin", "student")),
        (7l, ("jgonzal", "postdoc")),
        (5l, ("franklin", "prof")),
        (2l, ("istoica", "prof"))))

      // 创建边RDD，将元素封装为Edge对象,3个属性分别为边的起点，终点，边的关系属性
      val relationships: RDD[Edge[String]] = sc.parallelize(Seq(Edge(3l, 7l, "collab"),
        Edge(5l, 3l, "advisor"),
        Edge(2l, 5l, "colleague"),
        Edge(5l, 7l, "pi")))

      // 创建一个默认用户，接收指向丢失用户的关系
      val defaultUser = ("John Doe", "Missing")

      // 初始化图（需要输入顶点RDD，边RDD，默认节点）
      val graph = Graph(users, relationships, defaultUser)

      // 过滤出顶点中职业属性为 postdoc 的点
      graph.vertices.foreach(println)

      /** (点id,点属性(名称,职业))
       * (5,(franklin,prof))
       * (7,(jgonzal,postdoc))
       * (2,(istoica,prof))
       * (3,(rxin,student))
       * (7,(jgonzal,postdoc))
       */
      graph.vertices.filter { case (id, (name, pos)) =>
        pos == "postdoc"
      }.collect().foreach(println)

      /**
       * (7,(jgonzal,postdoc))
       */

      // 过滤出边中起点id 大于终点id的 边
      graph.edges.foreach(println)

      /** (起点,终点,边属性)
       * Edge(2,5,colleague)
       * Edge(5,7,pi)
       * Edge(5,3,advisor)
       * Edge(3,7,collab)
       */

      graph.edges.filter { e =>
        e.srcId > e.dstId
      }.collect().foreach(println)

      /**
       * Edge(5,3,advisor)
       */

      // 同上做相同过滤，只不过使用了case 提取器
      graph.edges.filter { case Edge(src, dst, prop) =>
        src > dst
      }.collect().foreach(println)

      /**
       * Edge(5,3,advisor)
       */

      // 过滤三元组 EdgeTriplet()
      graph.triplets.foreach(println)

      /** 起点                终点                边属性
       * ((2,(istoica,prof)),(5,(franklin,prof)),colleague)
       * ((3,(rxin,student)),(7,(jgonzal,postdoc)),collab)
       * ((5,(franklin,prof)),(3,(rxin,student)),advisor)
       * ((5,(franklin,prof)),(7,(jgonzal,postdoc)),pi)
       */
      graph.triplets.map { triplet =>
        s"${triplet.srcAttr._1} is the ${triplet.attr} of ${triplet.dstAttr._1}"
      }.collect().foreach(println)

      /**
       * rxin is the collab of jgonzal
       * franklin is the advisor of rxin
       * istoica is the colleague of franklin
       * franklin is the pi of jgonzal
       */
    } catch {
      case e: Throwable => e.printStackTrace()
    } finally {
      sc.stop()
    }
  }

}
