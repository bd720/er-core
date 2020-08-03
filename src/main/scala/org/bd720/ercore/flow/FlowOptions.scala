package org.wumiguo.ser.flow
import scala.collection.mutable
object FlowOptions {
  def getOptions(args: Array[String]): Map[String, String] = {
    val mMap = mutable.Map[String, String]()
    val optionSizeStr = args.filter(_.startsWith("optionSize=")).head
    val size = optionSizeStr.split("=")(1).toInt
    for (i <- (0 to size - 1)) {
      val optionStr = args.filter(_.startsWith("option" + i + "=")).head
      val kv = optionStr.split("=")(1).split(":")
      mMap.put(kv(0), kv(1))
    }
    mMap.toMap
  }
}
