package org.bd720.ercore.methods.datastructure
case class BlockWithComparisonSize(blockID : Int, comparisons : Double) extends Ordered[BlockWithComparisonSize]{
  def compare(that : BlockWithComparisonSize) : Int = {
    that.comparisons compare this.comparisons
  }
}
