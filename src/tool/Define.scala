package tool


/**
  * Created by stmatengss on 16-5-2.
  */
object Define {
  type Vid = Long
  type Pid = Int
  type Src = Long
  type Dst = Long
  type Dis = Long
  type Degree = Long

  type Vr = (Vid, Any)
  type Ep = (Vid, Vid)
  type Ed = (Ep, Long)
  type Tr = (Vr, Vr, Any)

  final val UNUSE:Byte = 0
  final val USE: Byte = 1
  final val SUCESS: Byte = 2
  final val OTHER: Byte = 3

  final val INF :Long = Long.MaxValue

  class EdgeDirection private (private val name: String) {
	def reverse: EdgeDirection = this match {
	  case EdgeDirection.In => EdgeDirection.Out
	  case EdgeDirection.Out => EdgeDirection.In
	  case EdgeDirection.Either => EdgeDirection.Either
	  case EdgeDirection.Both => EdgeDirection.Both
	}

	override def toString : String = "EdgesDirection." + name

	override  def equals(o: Any): Boolean = o match {
	  case other: EdgeDirection => other.name == name
		case _=>false
	}

	override  def hashCode: Int = name.hashCode
  }

  object EdgeDirection {
	/** Edges arriving at a vertex. */
	final val In: EdgeDirection = new EdgeDirection("In")

	/** Edges originating from a vertex. */
	final val Out: EdgeDirection = new EdgeDirection("Out")

	/** Edges originating from *or* arriving at a vertex of interest. */
	final val Either: EdgeDirection = new EdgeDirection("Either")

	/** Edges originating from *and* arriving at a vertex of interest. */
	final val Both: EdgeDirection = new EdgeDirection("Both")
  }

}
