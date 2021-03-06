package org.bd720.ercore.methods.datastructure
case class BlockClean(blockID: Int, profiles: Array[Set[Int]], var entropy: Double = -1, var clusterID: Integer = -1, blockingKey: String = "") extends BlockAbstract with Serializable {
  override def toString():String={
    "BlockClean(blockId:" +blockID+
      ",key=" + blockingKey +
      ",profiles=" +profiles.toList +
      ",ent="+entropy+",clusterId="+clusterID+")"
  }
  override def getComparisonSize(): Double = {
    val a = profiles.filter(_.nonEmpty)
    if (a.length > 1) {
      var comparisons: Double = 0
      var i = 0
      while (i < a.length) {
        var j = i + 1
        while (j < a.length) {
          comparisons += a(i).size.toDouble * a(j).size.toDouble
          j += 1
        }
        i += 1
      }
      comparisons
    }
    else {
      0
    }
  }
  override def getComparisons(): Set[(Int, Int)] = {
    var out: List[(Int, Int)] = Nil
    for (i <- profiles.indices) {
      for (j <- (i + 1) until profiles.length) {
        val a = profiles(i)
        val b = profiles(j)
        for (e1 <- a; e2 <- b) {
          if (e1 < e2) {
            out = (e1, e2) :: out
          }
          else {
            out = (e2, e1) :: out
          }
        }
      }
    }
    out.toSet
  }
}
