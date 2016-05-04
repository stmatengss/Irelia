package shrunk

import org.apache.spark.Partitioner
import tool.Define.Degree


/**
  * Created by stmatengss on 16-4-19.
  */
class PowerLyraParitioner(partitions: Int, threshHold: Int) extends Partitioner with Serializable{
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  val numParts = partitions
//  val randomNum=new scala.util.Random();
  val mixingPrime: Long = 1125899906842597L

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = {
	val k=key.asInstanceOf[(Long, Long)];
	k._2.toInt < threshHold match {
	  case true => (((k._1 * mixingPrime) % partitions+partitions) % partitions).toInt
	  case _ => (scala.util.Random.nextInt(partitions))
	}
  }

}

/**
  * Created by stmatengss on 16-4-19.
  */
class DynamicPowerLyraParitioner(partitions: Int, ratio: Int = 10) extends Partitioner with Serializable{
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  val numParts = partitions
  //  val randomNum=new scala.util.Random();
  val mixingPrime: Long = 1125899906842597L

  var sampleCount: Int = 0;
  val sampleEnd: Int = 100;

  val sample = Array.fill[Degree](sampleEnd)(0L)
  var idSet = scala.collection.mutable.Set()

  var threshHold: Int = 100

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = {
	val k=key.asInstanceOf[(Long, Long)];
	val id = k._1
	val degree = k._2

	if (idSet.exists(_==id)){

	} else {
	  sample(idSet.size) = degree
	  idSet += id
	}

	if (idSet.size == sampleEnd) {
	  threshHold = findElement(ratio)
	}

	degree.toInt < threshHold match {
	  case true => (((k._1 * mixingPrime) % partitions+partitions) % partitions).toInt
	  case _ => (scala.util.Random.nextInt(partitions))
	}
  }

  def findElement(no: Int): Int = {
	sample.sortWith({
	  case (x, y) => {x < y}
	})
	sample(ratio).toInt
  }

}


