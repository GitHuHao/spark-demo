package com.bigdata.demo.spark.graph

import com.bigdata.demo.spark.util.CommonSuit
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId}
import org.apache.spark.rdd.RDD
import org.junit.Test

class PageRankDemo {

  @Test
  def demo3(): Unit ={
    val conf = new SparkConf().setAppName("graphx-demo3").setMaster("local[*]")
    val sc = new SparkContext(conf)
    try {
      val followerFile = CommonSuit.getFile("graphx/followers.txt")
      val graph = GraphLoader.edgeListFile(sc, followerFile)
      val ranks = graph.pageRank(0.0001).vertices

      val userFile = CommonSuit.getFile("graphx/users.txt")
      val users = sc.textFile(userFile).map { line =>
        val fields = line.split(",")
        (fields(0).toLong, fields(1))
      }

      val rankByUsername: RDD[(String, Double)] = users.join(ranks).map {
        case (id, (name, rank)) => (name, rank)
      }

      rankByUsername.foreach(println(_))
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      sc.stop()
    }
  }

  @Test
  def wiki(): Unit ={
    val conf = new SparkConf().setAppName("wiki").setMaster("local[*]")
    val sc = new SparkContext(conf)
    try {
      val edgesFile = CommonSuit.getFile("wiki/graphx-wiki-edges.txt")
      val edges: RDD[Edge[Int]] = sc.textFile(edgesFile).map { line =>
        val fields = line.split("\t")
        Edge(fields(0).trim.toLong, fields(1).trim.toLong, 0)
      }

      val verticesFile = CommonSuit.getFile("wiki/graphx-wiki-vertices.txt")
      val vertices: RDD[(Long, String)] = sc.textFile(verticesFile).map { line =>
        val fields = line.split("\t")
        (fields(0).trim.toLong, fields(1).trim)
      }

      //顶点，边，默认点
      val graph = Graph(vertices, edges, "").persist()

      // 基于顶点直接关系排序，收敛阈值0.001
      val pageRankedGraph = graph.pageRank(0.001).cache()

      val titleAndPageRankedGraph: Graph[(Double, String), Int] = graph.outerJoinVertices(pageRankedGraph.vertices) {
        (verticeId, title, rank) => (rank.getOrElse(0.0), title)
      }

      titleAndPageRankedGraph.vertices.top(10) {
        Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
      }.foreach(x => println(s"${x._2._2}: ${x._2._1}"))
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      sc.stop()
    }

  }



}
