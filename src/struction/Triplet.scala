package struction

import tool.Define.Vid

/**
  * Created by stmatengss on 16-5-3.
  */
class Triplet[VD, ED] extends Edge[ED] {

  var srcAttr: VD = _
  var dstAttr: VD = _

  protected def set(other: Edge[ED]): Triplet[VD, ED] = {
	srcAttr = other.srcId
	dstAttr =other.dstId
	attr = other.attr
	this
  }

  def otherVertexAttr(id: Vid): VD ={
	if (srcId == v) dstAttr else { assert(dstId == v); srcAttr }
  }

  def vertexAttr(v: Vid): VD = {
	if (srcId == v) srcAttr else { assert(dstId == v); dstAttr }
  }

  override def toString: String = {((srcId, srcAttr), (dstId, dstAttr), attr).toString() }

  def toTuple: ((Vid, VD), (Vid, VD), ED) = ((srcId, srcAttr), (dstId, dstAttr), attr)

}
