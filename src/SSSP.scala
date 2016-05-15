/**
  * Created by stmatengss on 16-4-15.
  */

import shrunk._
import org.apache.spark.rdd._
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import Shrunk._
import tool.Define._

//val edges=sc.makeRDD(Array((8,1),(8,5),(9,8),(8,7),(7,3),(3,2),(6,3),(6,4)))
//val edgesLoad=sc.makeRDD(Array((1L,2L),(2L,3L),(2L,4L),(4L,5L),(6L,7L)))

object SSSP {

  //  val que=scala.collection.mutable.Queue[RDD[(Long,Long)]]();
  //  val resPro=scala.collection.mutable.ArrayBuffer[(Long,Long)]();
  val UNUSE: Int = 0;
  val USE: Int = 1;
  val SUCEESS: Int = 2;


  def run(edgesLoad: RDD[(Long, Long, Long)], Source:Vid , cluster: Long, directed: Boolean, th: Int, delta: Long): RDD[(Long, Long)] = {

	val LOWER_BOUND: Long = edgesLoad.count / cluster;
	var edges: RDD[(Vid, Vid, Dis)] = null;
	var clusterNum: Long = cluster;
	var activeVertex: RDD[(Vid, Dis)] = null;


	edges = edgesLoad


	var vertex: RDD[(Vid, Dis)] = toVertex(edges.map({
	  case (x, y, z) => (x, y)
	})).map({
	  case x => if(x == Source){
		(x.asInstanceOf[Long], 0L)
	  } else {
		(x.asInstanceOf[Long], INF)
	  }
	})

	var k: Int = 0

	while (k != INF) {

	  vertex = vertex.filter({
		case (v, x) => { (x/delta) >= k }
	  })

	  activeVertex = vertex.filter({
		case (v, x) => { (x/delta) == k }
	  })

	  while (activeVertex.count()>0 ) {

		val triplets = edges.map({
		  case (x, y, d) => (x, (y, d))
		}).join(activeVertex).map({
		  case (x, ((y, d), n)) => (y, (x, d, n))
		}).join(vertex).map({
		  case (y, ((x, d, n), m)) => ((x, n), (y, Math.min(m, n+d)), d)  //vital opeartor (relax)
		})

		val newActiveVertex = triplets.map({
		  case (x, y, z) => y
		}).subtract(vertex)

		vertex = triplets.flatMap({
		  case (x, y, d) => Iterator(x, y)
		}).distinct()

		activeVertex = vertex.filter({
		  case x => { (x.asInstanceOf[Long]/delta.asInstanceOf[Long]) == k }
		}).intersection(newActiveVertex)
	  }

	  k +=1
	}

	vertex
  }
}



