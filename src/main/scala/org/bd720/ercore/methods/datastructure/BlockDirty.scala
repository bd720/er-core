package org.bd720.ercore.methods.datastructure
case class BlockDirty(blockID: Int, profiles: Array[Set[Int]], var entropy: Double = -1, var clusterID: Integer = -1, blockingKey: String = "") extends BlockAbstract with Serializable {
  override def toString():String={
    "BlockDirty(blockId:" +blockID+
      ",key=" + blockingKey +
      ",profiles=" +profiles.toList +
      ",ent="+entropy+",clusterId="+clusterID+")"
  }
  override def getComparisonSize(): Double = {
    profiles.head.size.toDouble * (profiles.head.size.toDouble - 1)
  }
  override def getComparisons(): Set[(Int, Int)] = {
    profiles.head.toList.combinations(2).map { x =>
      if (x.head < x.last) {
        (x.head, x.last)
      }
      else {
        (x.last, x.head)
      }
    }.toSet
  }
}
