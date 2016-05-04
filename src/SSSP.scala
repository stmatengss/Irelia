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


  def bfsReplica(e: Array[(Long, Array[Long])], centers: Array[Long], nonCenters: Array[Long]): Iterator[((Long, Long), Int)] = {
	val adList = e.toArray;
	var flag = centers.length;
	val vertex = scala.collection.mutable.ArrayBuffer[(Long, Long)]();
	val coVertex = scala.collection.mutable.ArrayBuffer[(Long, Long)]();
	val resVertex = scala.collection.mutable.ArrayBuffer[(Long, Long)]();
	val used = Array.fill(adList.size)(true);
	val isReplica = Array.fill(adList.size)(true);
	val trans = adList.map(x => x._1).zipWithIndex.toMap;
	var root: Long = 0
	var now: Int = 0
	var son: Int = 0
	var rootTr: Int = 0;
	val que = scala.collection.mutable.Queue[Int]();
	centers.foreach({
	  case x => {
		isReplica(trans(x)) = false;
	  }
	}
	);
	for (root <- (centers ++ nonCenters)) {
	  flag -= 1;
	  rootTr = trans(root);
	  //println(root,rootTr);
	  if (used(rootTr)) {
		que.enqueue(rootTr);
		used(rootTr) = false;
		while (!que.isEmpty) {
		  now = que.dequeue();
		  for (j <- adList(now)._2) {
			//println(son)
			son = trans(j);
			if (used(son)) {
			  if (flag >= 0) {
				if (isReplica(son)) resVertex += ((root, j));
				else vertex += ((root, j));
			  };
			  else coVertex += ((root, j));
			  que.enqueue(son);
			  used(son) = false;
			}
		  }
		}
	  }
	}
	(vertex.map(x => (x, UNUSE))
		++ resVertex.map(x => (x, USE))
		++ coVertex.map(x => (x, SUCEESS))).toIterator;
  }

  def update(part: Iterator[((Long, Long), (Long, Long))]): Iterator[((Long, Long), Int)] = {
	val triplets = part.toArray;
	//	println("triplets:",triplets.foreach(print));
	val outDegrees = triplets.flatMap({ case (x, y) => Iterator(x, y) }).distinct.sortWith({
	  case (x, y) => (x._1 < y._1)
	});
	//	println("outDefrees:",outDegrees.foreach(print));
	val adList = triplets.map(x => (x._1._1, x._2._1)).flatMap({
	  case (src, dst) => Iterator((src, dst), (dst, src))
	}
	).groupBy(_._1).map(x => (x._1, x._2.map(_._2)));
	// TODO
	val partOutDegrees = adList.map(x => (x._1, x._2.length)).toArray.sortWith({
	  case (x, y) => (x._1 < y._1)
	});
	var centerCount = 0;
	val centers = scala.collection.mutable.ArrayBuffer[(Long)]();
	val nonCenters = scala.collection.mutable.ArrayBuffer[(Long)]();
	//	println("partOutDegrees:",partOutDegrees.foreach(print));
	// or Join?  If the partitioner will make the RDD in order
	outDegrees.zip(partOutDegrees).foreach(
	  {
		case (pro, now) => {
		  if (pro._2 == now._2) {
			nonCenters += (pro._1);
		  } else {
			centerCount += 1;
			centers += (pro._1);
		  }
		}
	  }
	)
	bfsReplica(adList.toArray, centers.toArray.sortWith({
	  case (numA, numB) => {
		numA < numB
	  }
	}), nonCenters.toArray);
  }

  def run(edgesLoad: RDD[(Long, Long)], Source:Vid , cluster: Long, directed: Boolean, th: Int, delta: Long): RDD[(Long, Long)] = {

	val que1 = scala.collection.mutable.Stack[RDD[(Long, Long)]]();
	val que2 = scala.collection.mutable.Stack[RDD[(Long, Long)]]();
	val LOWER_BOUND: Long = edgesLoad.count / cluster;
	var edges: RDD[(Long, Long)] = null;
	var resTmp: RDD[((Long, Long), Int)] = null;
	var clusterNum: Long = cluster;
	var activeVertex: RDD[(Long, Long)];

	if (directed == true) {
	  edges = edgesLoad.filter({ case (x, y) => {
		x < y
	  }
	  })
	} else {
	  edges = edgesLoad
	}

	val vertex = toVertex(edges).map({
	  case x => if(x == Source){
		(x, 0)
	  } else {
		(x, INF)
	  }
	})

	var k: Int = 0
	//	println("clusterNum",clusterNum);
	while (k != INF) {
//	  triplets = toTriplets(edges)
//	  triplets = triplets.partitionBy(new PowerLyraParitioner(clusterNum.toInt, th))
	  edges = edges.partitionBy(new PowerLyraParitioner(clusterNum.toInt, th))

	  activeVertex = vertex.filter({
		case x => { x/delta == k }
	  })

	  while (activeVertex.count() ) {

	  }

	  resTmp = triplets.mapPartitions(update, true);
	  resTmp.cache().count()
	}
  }
}



