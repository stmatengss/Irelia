package shrunk

import tool.Define
import Define._
import org.apache.spark.rdd.RDD

/**
  * Created by stmatengss on 16-5-3.
  */
object Shrunk {

  type  MyStack = scala.collection.mutable.Stack[Any]

  def toDirected(edges: RDD[(Vid, Vid)]): RDD[(Vid, Vid)] = {
	val res: RDD[(Vid, Vid)] = edges.filter({
	  case (src, dst) => {
		src < dst
	  }
	})
	res
  }

  def toUndirected(edges: RDD[(Vid, Vid)]): RDD[(Vid, Vid)] = {
	val res: RDD[(Vid, Vid)] = edges.flatMap({
	  case (src, dst) => Iterator((src, dst), (dst, src))
	})
	res
  }

  def outDegrees(edges: RDD[(Vid, Vid)]): RDD[(Vid, Degree)] = {
	val outDegree: RDD[(Vid, Degree)] = edges.flatMap({
	  case (src, dst) => Array((src, 1L), (dst, 1L))
	}).reduceByKey(_ + _)
	outDegree
  }

  def toVertex(edges: RDD[(Vid, Vid)]): RDD[Vid] = {
	val vertex = edges.flatMap({
	  case (src, dst) => Iterator(src, dst)
	}).distinct()
	vertex
  }

  def toTriplets(edges: RDD[(Vid, Vid)]): RDD[((Vid, Degree), (Vid, Degree))] = {
	val outDegree = outDegrees(edges)
	val triplets = edges.join(outDegree).map({
	  case (x, (y, n)) => (y, (x, n))
	}
	).join(outDegree).map({
	  case (x, (y, n)) => y._2 > n match {
		case true => ((x, n), y)
		case _ => (y, (x, n))
	  }
	})
	triplets
  }

  def apply
  (edgesLoad: RDD[(Vid, Vid)],
   clusterLoad: Long,
   threshold: Int)
  (
	update: Iterator[((Vid, Degree), (Vid, Degree))] => Iterator[((AnyVal , AnyVal), Byte)],
	gather: (Any, MyStack, MyStack, Any) => RDD[((AnyVal , AnyVal), Byte)],
  	finalGather: (Any, MyStack, MyStack, RDD[(Vid, Vid)]) => Any
  ) = {

	val LOWER_BOUND: Long = edgesLoad.count / clusterLoad;
	var clusterNum: Long = clusterLoad
	var edges: RDD[(Vid, Vid)] = null
	var triplets: RDD[((Vid, Degree), (Vid, Degree) )] = null
	var res: Any = null
	val que1 = new MyStack()
	val que2 = new MyStack()

	edges = edgesLoad

	while (clusterNum >=1 ) {

	  triplets = toTriplets(edges)
	  triplets.partitionBy(new PowerLyraParitioner(clusterNum.toInt, threshold))

	  val resTmp = triplets.mapPartitions(update, true)

	  edges = resTmp.filter({
		case (x, y) => {y == UNUSE}
	  }).map({
		case (x, y) => {(x._1.toString.toLong, x._1.toString.toLong)}
	  }).cache();

	  gather(res, que1, que2, resTmp)

	  if (clusterNum == 1) {
		clusterNum = 0
	  } else {
		clusterNum = Math.max(1, Math.min(clusterNum, edges.count / LOWER_BOUND))
	  }
	}
	finalGather(res, que1, que2, edges)
	res
  }
}
