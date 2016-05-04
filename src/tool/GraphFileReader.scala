package tool


import org.apache.commons.logging.Log
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.impl.{EdgePartitionBuilder, GraphImpl}
import org.apache.spark.rdd.RDD
//import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
  * Created by stmatengss on 16-5-2.
  */
object GraphFileReader {

  def edgesListFile(
				   sc: SparkContext,
				   path: String,
				   canonicalOrientation: Boolean = false,
				   numEdgeParitions: Int = -1
				   )
  : RDD[Any]={

	val startTime = System.currentTimeMillis()

	val lines = if (numEdgeParitions > 0) {
	  sc.textFile(path, numEdgeParitions).coalesce(numEdgeParitions)
	} else {
	  sc.textFile(path)
	}

	val edges: RDD[Any] = lines.mapPartitionsWithIndex{
	  case (pid, iter)=>
		val res = ArrayBuffer[Any]()
		iter.foreach{
		  line => if (!line.isEmpty && line(0) != '#') {
			val lineArray = line.split("\\s+")
			if (lineArray.length < 2) {
			  throw new Exception
			}
			val srcId = lineArray(0).toLong
			val dstId = lineArray(1).toLong
			if (lineArray.length > 2) {
			  val attr = lineArray(2).toLong
			  res+= (((srcId, dstId), attr))
			} else {
			  res+= ((srcId, dstId))
			}
		  }
		}
		res.toIterator
	}
	println("It took %d ms to load the edges".format(System.currentTimeMillis()-startTime))
	edges.persist(StorageLevel.MEMORY_ONLY).setName("GraphLoader Succeed - edges (%s)".format(path))
	edges.count()
	edges
  }

  def testFileReader() = {

	val conf = new SparkConf().setAppName("test")
	val sc = new SparkContext(conf)

	val res = edgesListFile(
	  sc,
	  "src/data/sssp_small_graph"
	)
	res.foreach(println(_))
  }

  def main(args: Array[String]) {
	testFileReader()
  }

}
