package tool

/**
  * Created by stmatengss on 16-5-3.
  */
object GraphLog {

  def Alert() = {
	println("**********************************")
  }

  def Log(a: AnyVal, name: String) = {
	Alert()
	println(name + "==" + a.toString)
  }

  def Log(a: Tuple2[AnyVal, AnyVal], name: String) = {
	Alert()
	printf(name + "==" + a._1.toString + "," + a._2.toString)
  }

  def Log(a: Tuple3[AnyVal, AnyVal, AnyVal], name: String) = {
	Alert()
	printf(name + "==" + a._1.toString + "," + a._2.toString + "," + a._3.toString)
  }

  def Log(a: Tuple4[AnyVal, AnyVal, AnyVal, AnyVal], name: String) = {
	Alert()
	printf(name + "==" + a._1.toString + "," + a._2.toString + "," + a._3.toString + "," + a._4.toString)
  }


  def Log(a: Traversable[Any], name: String) = {
	Alert()
	println(name + "::")
	//	  var counter = 0
	a.foreach({
	  x => println(name + x.toString)
	})
  }

}
