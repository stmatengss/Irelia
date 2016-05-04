package bench

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd._
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by stmatengss on 16-4-29.
  */
object GraphxSSSP {


  def main(args:Array[String])={
//	val conf = new SparkConf().setAppName("Simple Application").set("spark.worker.cores","16");
//	val sc = new SparkContext(conf);
//	Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//	Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
	val res = run(
	  Array( ((1L,2L),1L), ((1L,3L),4L), ((2L,3L),2L), ((1L,4L),7L), ((3L,4L),2L), ((2L,4L), 1L), ((6L,7L),1L), ((4L,5L),1L) ) ,
	  1L,
      2L
	);
	res.foreach(println(_))
  }

//  def Long = Long
//  def Long = Long

  def run(edges: Array[((Long,Long),Long)], source: Long = 1L, delta: Long = 2L):Array[(Long, Long)] = {

	def transK(w:Long, delta: Long): Long = {w/delta}



	val dis = scala.collection.mutable.Map[Long, Long]()
	dis++= edges.flatMap({
	  case ((src,dst), attr)=>Iterator((src, Long.MaxValue) ,(dst, Long.MaxValue))
	}).distinct.map({case x=>{
	  if (x._1==source) (x._1,0L)
	  else x
	}}).toMap



	val adList = edges.flatMap({
	  case ((src,dst),attr)=>Iterator((src, (dst, attr)), (dst, (src, attr)))
	}).groupBy(_._1).map(x=>(x._1,(x._2.map(_._2) )))

	var k:Long = 0L


	def processBucket()={
	  val Bk =dis.filter({case x=>transK(x._2, delta) == k}).map(_._1)
//	  Bk.foreach(println(_))
	  var A = Bk.toSet
	  while (A.size> 0){
		val change = scala.collection.mutable.Set[Long]()

		for (a<-A){
		  val u = a
//		  println("u==",u)
		  for (b<-adList(u)){
			val v = b._1
			val wei = b._2
//			println("   v==",v,wei)
			val before = dis(v)
			dis(v) = Math.min(before, dis(u)+wei)
			if (dis(v) < before)change+=(v)
		  }
		}
		A = dis.filter({case x=>transK(x._2, delta) == k}).map(_._1).toSet.intersect(change.toArray.toSet)
//		println("A-size==",A.size)
	  }

	}

	while( k<Long.MaxValue ){
//	  println("k=",k)
//	  dis.foreach(println(_))
	  processBucket()
	  val less = dis.map({case x=>{transK(x._2, delta)}}).toArray.filter( _>k)
//	  println("len=",less.length)
	  if (less.length == 0) k= Long.MaxValue
	  else k = less.min
	}
	dis.toArray
  }
}
