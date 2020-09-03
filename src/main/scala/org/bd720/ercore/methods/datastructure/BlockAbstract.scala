package org.bd720.ercore.methods.datastructure
trait BlockAbstract extends Ordered[BlockAbstract] {
  val blockingKey: String
  val blockID: Int
  var entropy: Double
  var clusterID: Integer
  val profiles: Array[Set[Int]]
  def size: Double = profiles.map(_.size.toDouble).sum
  def getComparisonSize(): Double
  def getAllProfiles: Array[Int] = profiles.flatten
  def getComparisons() : Set[(Int, Int)]
  def compare(that: BlockAbstract): Int = {
    this.getComparisonSize() compare that.getComparisonSize()
  }
}
