package com.bigdata.demo.spark.graph

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, EdgeContext, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.junit.Test

class PergelDemo extends Serializable {

  @Test
  def demo2(): Unit = {
    val conf = new SparkConf().setAppName("demo2").setMaster("local[*]")
    val sc = new SparkContext(conf)
    try {
      val vertexArr = Seq(
        (1l, ("Alice", 28)),
        (2l, ("Bob", 27)),
        (3l, ("Charlie", 65)),
        (4l, ("David", 42)),
        (5l, ("Ed", 55)),
        (6l, ("Fran", 50))
      )

      var edgeArr = Array(
        Edge(2l, 1l, 7),
        Edge(2l, 4l, 2),
        Edge(3l, 2l, 4),
        Edge(3l, 6l, 3),
        Edge(4l, 1l, 1),
        Edge(5l, 2l, 2),
        Edge(5l, 3l, 8),
        Edge(5l, 6l, 3)
      )

      // 准备 顶点，边RDD
      val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArr)
      val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArr)

      // 初始化图
      val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

      println("过滤出age>30 的顶点")
      graph.vertices.filter { case (_, (_, age)) => age > 30 }.collect().foreach {
        case (id, (name, age)) => println(s"${name} is ${age}")
      }

      println("过滤出属性大于5的边")
      graph.edges.filter(e => e.attr > 5).collect().foreach { e =>
        println(s"${e.srcId} to ${e.dstId} attr: ${e.attr}")
      }

      println("过滤出错边属性大于5的三元组")
      graph.triplets.filter(t => t.attr > 5).collect().foreach { t =>
        println(s"${t.srcAttr._1} likes ${t.dstAttr._1}")
      }

      val maxDegree = (a: (VertexId, Int), b: (VertexId, Int)) => if (a._2 > b._2) a else b
      val minDegree = (a: (VertexId, Int), b: (VertexId, Int)) => if (a._2 > b._2) b else a

      println("出度 out-degrees")
      val outDegrees = graph.outDegrees
      outDegrees.foreach(println(_))

      val maxOut: (VertexId, Int) = outDegrees.reduce(maxDegree)
      val minOut: (VertexId, Int) = outDegrees.reduce(minDegree)
      println(s"max: ${maxOut}\tmin: ${minOut}")

      println("入度 in-degrees")
      val inDegrees = graph.inDegrees
      inDegrees.foreach(println(_))

      val maxIn: (VertexId, Int) = inDegrees.reduce(maxDegree)
      val minIn: (VertexId, Int) = inDegrees.reduce(minDegree)
      println(s"max: ${maxIn}\tmin: ${minIn}")

      println("度数 degrees")
      val degrees = graph.degrees
      degrees.foreach(println(_))

      val maxDeg: (VertexId, Int) = degrees.reduce(maxDegree)
      val minDeg: (VertexId, Int) = degrees.reduce(minDegree)
      println(s"max: ${maxDeg}\tmin: ${minDeg}")

      println("图顶点转换操作")
      graph.mapVertices { case (id, (name, age)) => (id, (name, age + 10)) }.vertices.foreach { v =>
        println(s"${v._2._1} is ${v._2._2}")
      }

      println("图边转换操作")
      graph.mapEdges { e => e.attr * 2 }.edges.foreach { e =>
        println(s"${e.srcId} to ${e.dstId} attr: ${e.attr}")
      }

      println("三元组转换操作")
      graph.mapTriplets { t => t.attr + 10 }.triplets.foreach { t =>
        println(s"${t.srcId}[${t.srcAttr}] ${t.attr} ${t.dstId}[${t.dstAttr}]")
      }

      println("顶点 age>30的子图")
      val subGraph = graph.subgraph(vpred = (id, vd) => vd._2 >= 30)

      println("子图的顶点")
      subGraph.vertices.foreach { v =>
        println(s"${v._2._1} is ${v._2._2}")
      }

      println("子图的边")
      subGraph.edges.foreach { e =>
        println(s"${e.srcId} to ${e.dstId} attr: ${e.attr}")
      }

      case class User(name: String, age: Int, inDegree: Int, outDegree: Int)

      // User 为点，Int 为边
      val initialUserGraph: Graph[User, Int] = graph.mapVertices { case (id, (name, age)) => User(name, age, 0, 0) }

      println("user图与入度、出度连表")
      val userGraph: Graph[User, Int] = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
        case (id, user, inDegreeOption) => User(user.name, user.age, inDegreeOption.getOrElse(0), user.outDegree)
      }.outerJoinVertices(initialUserGraph.outDegrees) {
        case (id, user, outDegreeOption) => User(user.name, user.age, user.inDegree, outDegreeOption.getOrElse(0))
      }

      userGraph.vertices.foreach { v =>
        println(s"${v._2.name} inDegree: ${v._2.inDegree} outDegree: ${v._2.outDegree}")
      }

      println("入度与出度相同的点")
      userGraph.vertices.filter {
        case (id, user) => user.inDegree == user.outDegree
      }.foreach {
        case (id, user) => println(user.name)
      }

      /**
       * 1. 老版本
       * val graph: Graph[Int, Float] = ...
       * def msgFun(triplet: Triplet[Int, Float]): Iterator[(Int, String)] = {
       * Iterator((triplet.dstId, "Hi"))
       * }
       * def reduceFun(a: String, b: String): String = a + " " + b
       * val result = graph.mapReduceTriplets[String](msgFun, reduceFun)
       *
       * 2. 新版本
       * val graph: Graph[Int, Float] = ...
       * def msgFun(triplet: EdgeContext[Int, Float, String]) {
       *   triplet.sendToDst("Hi")
       * }
       * def reduceFun(a: String, b: String): String = a + " " + b
       * val result = graph.aggregateMessages[String](msgFun, reduceFun)
       *
       * 3.公式
       * def aggregateMessages[A: ClassTag 输出类型](
       * sendMsg: EdgeContext[VD顶点类型, ED边类型, A输出絫] => Unit,  将源顶点属性发送给目标顶点 （此处发送的是源顶点的名称和年龄）
       * mergeMsg: (A输出类型, A输出类型) => A输出类型, // 目标顶点合并消息，此处将源顶点的年龄与目标点对比，取较大者
       * tripletFields: TripletFields = TripletFields.All)
       * : VertexRDD[A] = {
       * aggregateMessagesWithActiveSet(sendMsg, mergeMsg, tripletFields, None)
       * }
       */
      println("最大追求者")
      val oldestFollowers: VertexRDD[(String, Int)] = userGraph.aggregateMessages[(String, Int)](
        sendMsg = (triplet: EdgeContext[User, Int, (String, Int)]) => triplet.sendToDst(triplet.srcAttr.name, triplet.srcAttr.age),
        mergeMsg = (userInfo1: (String, Int), userInfo2: (String, Int)) => if (userInfo1._2 > userInfo2._2) userInfo1 else userInfo2
      )

      oldestFollowers.foreach(println(_))

      userGraph.vertices.leftJoin(oldestFollowers) {(id,user, oldestFollower) =>
        oldestFollower match {
          case None => s"${user.name} does not have any followers"
          case Some((name, _)) => s"${name} is the oldest follower of ${user.name}"
        }
      }.foreach(println(_))

      println("5到各点最短路径")
      val sourceId:VertexId = 5l
      /**
       * def pregel[A: ClassTag](
       * initialMsg: A,
       * maxIterations: Int = Int.MaxValue,
       * activeDirection: EdgeDirection = EdgeDirection.Either)(
       * vprog: (VertexId, VD, A) => VD,
       * sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
       * mergeMsg: (A, A) => A)
       * : Graph[VD, ED] = {
       * Pregel(graph, initialMsg, maxIterations, activeDirection)(vprog, sendMsg, mergeMsg)
       * }
       */
      val graph5 = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
      val shortestTo5:Graph[Double,Int] = graph5.pregel(Double.PositiveInfinity)( // initialMsg 初始化5之外点，初始化接收值
        vprog = (id, dist, newDist) => math.min(dist, newDist),  // vprog
        sendMsg = triplet => {
          if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
            Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
          } else {
            Iterator.empty
          }
        },
        mergeMsg = (a, b) => math.min(a, b) // 合并
      )

      shortestTo5.vertices.foreach(println(_))

    } catch {
      case e: Throwable => e.printStackTrace()
    } finally {
      sc.stop()
    }
  }

}
