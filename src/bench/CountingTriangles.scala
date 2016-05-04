package bench

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.graphx._

/**
  * Created by stmatengss on 16-4-18.
  */
object CountingTriangles {
  def main(args: Array[String]){
	val name=args(0);
	val num=args(1).toInt;
	val conf = new SparkConf().setAppName("Simple Application").set("spark.worker.cores","16");
	val sc = new SparkContext(conf);
	Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
	Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

	val graph = GraphLoader.edgeListFile(sc, "/home/mateng/spark/"+name,numEdgePartitions=num,canonicalOrientation=true).partitionBy(PartitionStrategy.RandomVertexCut);

	val begin=System.currentTimeMillis;
	val v = graph.triangleCount().vertices;
	val cc=v.map(_._2).reduce(_+_);

	println("Counter is "+cc.toString);

	println("Time is ",System.currentTimeMillis-begin);
  }
}
