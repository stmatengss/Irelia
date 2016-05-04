/**
  * Created by stmatengss on 16-4-13.
  */

import org.apache.spark.rdd._
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import WCC._
import CT._


object Test {
  def main(args: Array[String]): Unit = {
	println("test");
	Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
	Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
	args(3) match {
	  	case "wcc" => testWCC(args)
		case "ct" => testCT(args)
	};
	//           println("result::",res.foreach(x=>{println("vital:::",x) }))
	//            res.foreach(x=>{if(x._1==0L)counter+=1})
	//            println("counter::",counter);
	//            val line =sc.makeRDD(args);
	//            line.map((_,1)).reduceByKey(_+_).collect.foreach(println);
	//            sc.stop();
  }

  def testCT(args: Array[String]) = {
	val conf = new SparkConf().setAppName("test");
	val sc = new SparkContext(conf);
	val txt = sc.textFile(args(2));
	val edgesLoad = args(0) match {
	  case "1" => txt.map(x => {
		val t = (x.split(' ')).map(_.toLong);
		(t(0), t(1))
	  }).filter({ case (x, y) => {
		x < y
	  }
	  });
	  case "2" => txt.map(x => {
		val t = (x.split('\t')).map(_.toLong);
		(t(0), t(1))
	  }).filter({ case (x, y) => {
		x < y
	  }
	  });
	}
	edgesLoad.cache().count();
	//            val edgesLoad=sc.makeRDD(Array((1L,2L),(2L,3L),(2L,4L),(4L,5L),(6L,7L)))
	val begin = System.currentTimeMillis();
	val res = CT.run(edgesLoad, args(1).toInt, false, 100)
	val end = System.currentTimeMillis();
	println("time::", (end - begin).toString + "ms")
	println("Test::", res);
  }

  def testWCC(args: Array[String]) = {

	val conf = new SparkConf().setAppName("test");
	val sc = new SparkContext(conf);
	val txt = sc.textFile(args(2));
	val edgesLoad = args(0) match {
	  case "1" => txt.map(x => {
		val t = (x.split(' ')).map(_.toLong);
		(t(0), t(1))
	  }).filter({ case (x, y) => {
		x < y
	  }
	  });
	  case "2" => txt.map(x => {
		val t = (x.split('\t')).map(_.toLong);
		(t(0), t(1))
	  }).filter({ case (x, y) => {
		x < y
	  }
	  });
	}
	edgesLoad.cache().count();
	//            val edgesLoad=sc.makeRDD(Array((1L,2L),(2L,3L),(2L,4L),(4L,5L),(6L,7L)))
	val begin = System.currentTimeMillis();
	val res = WCC.run(edgesLoad, args(1).toInt, false, 100).collect()
	val end = System.currentTimeMillis();
	println("time::", (end - begin).toString + "ms")
	println("Test::", res.take(20).foreach(println(_)));
  }
}
