
package bench

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import tool.Define.Vid
import tool.GraphFileReader._
import tool.GraphLog
import shrunk.Shrunk._

/**
  * Created by stmatengss on 16-5-4.
  */
object TestShrunking {

  def run() = {
	val conf = new SparkConf().setAppName("test")
	val sc = new SparkContext(conf)

	val res = edgesListFile(
	  sc,
	  "src/data/ct_small_graph"
	)

	val edges: RDD[(Vid, Vid)] = res.map(_.asInstanceOf[Tuple2[Vid, Vid]])

	GraphLog.Log(res.collect(), "Res")

	val triplets = toTriplets(edges)

	GraphLog.Log(triplets.collect(), "triplets")

	val degree = outDegrees(edges)

	GraphLog.Log(degree.collect(), "degree")

  }

  def main(args: Array[String]) {
	run()
  }
}
