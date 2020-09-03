package org.bd720.ercore.methods.datastructure
trait PBlockAbstract extends Ordered[BlockAbstract] {
  val blockingKey: String
  val blockID: Long
  var entropy: Double
  var clusterID: Integer
  val profiles: Array[Set[(Long, Profile)]]
  def size: Double = profiles.map(_.size.toDouble).sum
  def getComparisonSize(): Double
  def getAllProfiles: Array[(Long, Profile)] = profiles.flatten
  def getComparisons() : Set[(Long, Long)]
  def compare(that: BlockAbstract): Int = {
    this.getComparisonSize() compare that.getComparisonSize()
  }
}
