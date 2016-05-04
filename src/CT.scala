/**
  * Created by stmatengss on 16-4-13.
  */

import org.apache.spark.rdd._
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import shrunk.PowerLyraParitioner

import scala.collection

//val edges=sc.makeRDD(Array((8,1),(8,5),(9,8),(8,7),(7,3),(3,2),(6,3),(6,4)))
//val edgesLoad=sc.makeRDD(Array((1L,2L),(2L,3L),(2L,4L),(4L,5L),(6L,7L)))

object CT {

  //  val que=scala.collection.mutable.Queue[RDD[(Long,Long)]]();
  //  val resPro=scala.collection.mutable.ArrayBuffer[(Long,Long)]();
  val SUM: Int = 0;
  val RES: Int = -1;
  val CHECK: Int = 1;

  def countingReplica(e: Array[(Long, Array[Long])], center: Array[Long], nonCenter: Array[Long]): Iterator[((Long,Long),Int)]= {
	val adList = e;
	var res:Long = 0;
	val trans = adList.map(x => x._1).zipWithIndex.toMap;
	var root: Int = 0;
	var son: Int = 0;
	var countDo: Long = 0;
	var countTri: Long = 0;
	var count: Long=0;
	var isCenter = center.toSet;
	val check = scala.collection.mutable.ArrayBuffer[((Long,Long),Int)]();
	var tmp: Array[Long] = null;
/*	for (u <- (nonCenters)) {
	  root = trans(u);

	  for (v <- adList(root)._2 if v > u) {
		for (w <- adList(root)._2 if w > u;
			 if adList(trans(v))._2.exists(_ == w)) {
		  count += 1;
		}
	  }
	  resVertex += ((u, count));
	}*/
		//a new method to calculate
	for(u<-nonCenter++center ) {
	  root = trans(u);
	  countDo = 0;
	  countTri = 0;
	  val flag = isCenter(u);
	  for (v <- adList(root)._2 ) {
		son = trans(v);
		tmp = null;
//		if(v > u){
		  tmp = adList(son)._2.intersect(adList(root)._2);
		  countDo += tmp.length;
//		}
		if ( flag ){
//		  if (tmp == null)tmp = adList(son)._2.intersect(adList(root)._2);
		  if (!isCenter(v)) {
			val tmp2 = center.intersect(adList(son)._2).toSet -- tmp;
			for (w <- tmp2 if w != u && u < w) {
			  check += (((u, w), CHECK));
			}
		  }else{
			for (w <- tmp if isCenter(w)){
			  countTri +=1L;
			}
		  }
		}

//		for(w <- adList(son)._2 if isCenter(w))check+=(((u, w),CHECK));
	  }
	  res += countDo- countTri;
	}
//	res;
	res/=6L;
//	println("Res=="+res.toString);
	Iterator(((res,res), SUM))++check.toIterator;
  }

  def update(part: Iterator[((Long, Long), (Long, Long))]): Iterator[((Long, Long),Int)] = {
	val triplets = part.toArray;

//	println("triplets:",triplets.foreach(print));
	val outDegrees = triplets.flatMap({ case (x, y) => Iterator(x, y) }).distinct.sortWith({
	  case (x, y) => (x._1 < y._1)
	});
//		println("outDefrees:",outDegrees.foreach(print));
	val edges=triplets.map(x => (x._1._1, x._2._1));
	val adList=edges.flatMap({
	  case (src, dst) => Iterator((src, dst), (dst, src))
	}).groupBy(_._1).map(x => (x._1, x._2.map(_._2)));
	// TODO
	val partOutDegrees = adList.map({
	  case (x, y) => (x, y.length)
	}).toArray.sortWith({
	  case (x, y) => (x._1 < y._1)
	});
	val centers = scala.collection.mutable.ArrayBuffer[(Long)]();
	val nonCenters = scala.collection.mutable.ArrayBuffer[(Long)]();
//		println("partOutDegrees:",partOutDegrees.foreach(print));
	// or Join?  If the partitioner will make the RDD in order
	outDegrees.zip(partOutDegrees).foreach({
		case (pro, now) => {
		  if (pro._2 == now._2) {
			nonCenters += (pro._1);
		  } else {
			centers += (pro._1);
		  }
		}
	  }
	)
	val isCenter=centers.toSet;

	val less=edges.filter({
	  case (x,y)=>{isCenter(x)&&isCenter(y)}
	}).map({
	  case(src,dst)=>
		if(src<dst) ((src, dst),RES)
		else ((dst, src),RES)
	});

	countingReplica(adList.toArray, centers.toArray, nonCenters.toArray)++less.toIterator;
  }

  def run(edgesLoad: RDD[(Long, Long)], cluster: Long, directed: Boolean, th:Int ): Long= {

	val LOWER_BOUND: Long = edgesLoad.count / cluster;
	var edges: RDD[(Long, Long)] = null;
	var check: RDD[((Long, Long), Int)] = null;
	var resTmp: RDD[((Long, Long), Int)] = null;
	var clusterNum: Long = cluster;
	var resPro: Long=0;
	var resCheck: Long=0;
	var res:Long=0;
	var triplets: RDD[((Long, Long), (Long, Long))]=null;

	if (directed == true) {
	  edges = edgesLoad.filter({
		case (x, y) => {x < y}
	  })
	} else {
	  edges = edgesLoad
	}
	//	println("clusterNum",clusterNum);
	while (clusterNum >= 1) {
//	  edges = edges.partitionBy(new HashPartitioner(clusterNum.toInt));
	  //	  clusterNum=clusterNum/2;
	  //	  println("clusterNum",clusterNum);
	  val outDegrees: RDD[(Long, Long)] = edges.flatMap({
		case (src, dst) => Array((src.toLong, 1L), (dst.toLong, 1L))
	  }
	  ).reduceByKey(_ + _);
	  //	  println("edges:",edges.foreach(println(_)))
	  triplets = edges.join(outDegrees).map(
		{ case (x, (y, n)) => (y, (x, n)) }
	  ).join(outDegrees).map(
		{ case (x, (y, n)) => ((x, n), y) }
	  )
	  triplets = triplets.map({
		case ((src, srcAttr),(dst, dstAttr)) => {
		  if(srcAttr > dstAttr) ((dst, dstAttr),(src, srcAttr))
		  else ((src, srcAttr),(dst, dstAttr))
		}
	  });
//	  triplets = triplets.partitionBy(new HashPartitioner(clusterNum.toInt));
	  triplets = triplets.partitionBy(new PowerLyraParitioner(clusterNum.toInt,th));
	  triplets.cache().count()
//	  println("triplets:",triplets.foreach(println(_)))
	  resTmp = triplets.mapPartitions(update, true);
	  resTmp.cache().count()
	  edges = resTmp.filter({
		case (x, y) => {y == RES}
	  }).map(_._1).cache();
	  resPro = resTmp.filter({
		case (x, y) => { y == SUM}
	  }).keys.keys.reduce(_+_);
	  check = resTmp.filter({
		case (x,y) => { y == CHECK}
	  }).reduceByKey(_+_).cache();

	  resCheck=0;
	  if(!edges.isEmpty && !check.isEmpty){
		val test = edges.map({case (x,y)=>((x,y), 1L)})
//		println("Test",test.foreach(print(_)))
//		println("edges:",edges.foreach(x=>{print(x);}))
//		println(edges.glom.collect().foreach(x=>{println();x.foreach(print)}))
//		println("check:",check.foreach(print))
//		println(check.glom.collect().foreach(x=>{println();x.foreach(print)}))
		val join = test.partitionBy(new HashPartitioner(clusterNum.toInt)).join(check.partitionBy(new HashPartitioner(clusterNum.toInt)));
//		val join = test.join(check , new HashPartitioner(clusterNum.toInt));
//		val join = test.join(check , new shrunk.PowerLyraParitioner(clusterNum.toInt,th));
		//		println("Join:",join.foreach(print))
//		val test2=join.filter(x=>x._2._1.isInstanceOf[Long] && x._2._2.isInstanceOf[Long])
		if(!join.isEmpty())resCheck = join.map(_._2._2).reduce(_+_)
	  };
	  //TODO more profermance
//	  println("edges:",edges.foreach(print))
//	  println("check:",check.foreach(print))
	  println("resPro:",resPro)
	  println("resCheck:",resCheck)
	  res+=resPro+resCheck;
	  if (clusterNum == 1) clusterNum = 0;
	  else clusterNum = Math.max(1, Math.min(clusterNum, edges.count / LOWER_BOUND));
	  println("ClusterNum=", clusterNum, edges.count());
//	  println("edges:",edges.foreach(print(_)))
	  //println("resPro:",resPro.foreach(println(_)))
	}
	//	println("edges::",edges.foreach(print(_)));
	//	println("Length:",que.length)
	res
  }
}


