package struction


import tool.Define
import Define._

/**
  * Created by stmatengss on 16-5-3.
  */
class Edge[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED] (
				  var srcId: Vid = 0,
				  var dstId: Vid = 0,
				  var attr: ED = null.asInstanceOf[ED] ) {
  def otherVertex(v: Vid): Vid = {
	if (srcId == v ) dstId  else { assert(dstId == v); srcId }
  }

  def relativeDirection(v: Vid): EdgeDirection = {
	if (v == srcId) EdgeDirection.Out else  {assert(v == dstId); EdgeDirection.In}
  }

  override def toString: String = {(srcId, dstId, attr).toString() }

  def toTuple: (Vid, Vid, ED) = (srcId, dstId, attr)
}


object  Edge {
  def lexicalOrder[ED] = new Ordering[Edge[ED] ] {
	override  def compare(a: Edge[ED], b:Edge[ED]): Int = {
	  if (a.srcId == b.srcId) {
		if (a.dstId == b.dstId)  0
		else if (a.dstId > b.dstId) 1
		else -1
	  }
	  else {
		if (a.srcId > b.srcId) 1
		else -1
	  }
	}
  }

  def swap[ED](data: Array[Edge[ED]], pos1: Int, pos2:Int) = {
	val tmp = data(pos1)
	data(pos1) = data(pos2)
	data(pos2) = tmp
  }


}

