package org.bd720.ercore.methods.datastructure
trait BlockAbstract extends Ordered[BlockAbstract] {
  val blockingKey: String
  val blockID: Int
  var entropy: Double
  var clusterID: Integer
  val profiles: Array[Set[Int]]
  def size: Double = profiles.map(_.size.toDouble).sum
  /* Return the number of comparisons entailed by this block */
  def getComparisonSize(): Double
  /* Returns all profiles */
  def getAllProfiles: Array[Int] = profiles.flatten
  /* Returns all the comparisons */
  def getComparisons() : Set[(Int, Int)]
  def compare(that: BlockAbstract): Int = {
    this.getComparisonSize() compare that.getComparisonSize()
  }
}
