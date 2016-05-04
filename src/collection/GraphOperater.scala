package collection

import tool.Define
import Define._
import org.apache.spark.rdd.RDD
import tool.Define

import scala.collection.mutable.ArrayBuffer

/**
  * Created by stmatengss on 16-5-3.
  */
object GraphOperater {
  def toAdjacencyList(edges: Array[(Vid, Vid)]): Array[(Vid, Array[Vid])] = {
	val adList = edges.flatMap({
	  case (src, dst) => Array((src, dst), (dst,src))
	}).groupBy(_._1).map(x => (x._1, x._2.map(_._2) ) )
	adList.toArray
  }

  def outDegrees(adList: Array[(Vid, Array[Vid])]): Array[(Vid, Degree)] = {
	adList.map({
	  case (v, it) => (v, it.length.toLong)
	})
  }

  def leftEdges(edges: Array[(Vid, Vid)], isReplica: Set[Vid]): Array[((Vid, Vid), Byte)] = {
	edges.filter({
	  case (src, dst) => {isReplica(src) && isReplica(dst) }
	}).map({
	  case (src, dst) =>
		if (src < dst) ( (src, dst), UNUSE)
		else ((dst, src),UNUSE)
	})
  }

  def classfyReplica(degree: Array[(Vid, Degree)], nowDegree: Array[(Vid, Degree)],
					replicaSet: ArrayBuffer[Vid], nonReplicaSet: ArrayBuffer[Vid] ): Unit = {
	degree.zip(nowDegree).foreach({
	  case (pre, now) => {
		if(pre._2 == now._2) {
		  nonReplicaSet += (pre._1)
		} else {
		  replicaSet += (pre._1)
		}
	  }
	})
  }



}
