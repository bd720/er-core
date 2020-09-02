package org.bd720.ercore.methods.datastructure
trait PBlockAbstract extends Ordered[BlockAbstract] {
  val blockingKey: String
  val blockID: Long
  var entropy: Double
  var clusterID: Integer
  val profiles: Array[Set[(Long, Profile)]]//: Array[Set[Long]]
  def size: Double = profiles.map(_.size.toDouble).sum
  /* Return the number of comparisons entailed by this block */
  def getComparisonSize(): Double
  /* Returns all profiles */
  def getAllProfiles: Array[(Long, Profile)] = profiles.flatten
  /* Returns all the comparisons */
  def getComparisons() : Set[(Long, Long)]
  def compare(that: BlockAbstract): Int = {
    this.getComparisonSize() compare that.getComparisonSize()
  }
}
