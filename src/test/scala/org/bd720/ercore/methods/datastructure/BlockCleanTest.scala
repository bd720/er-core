package org.bd720.ercore.methods.datastructure
import org.scalatest.FlatSpec
class BlockCleanTest extends FlatSpec {
  it should " create clean block 1 " in {
    val block = BlockClean(0, Array[Set[Int]](Set[Int](1, 9, 4, 5), Set[Int](2, 3, 7)), 0.8, 1, "bigdata_111")
    assertResult(Set((2, 5), (7, 9), (3, 9), (3, 4), (4, 7), (3, 5), (2, 9), (5, 7), (1, 3), (2, 4), (1, 7), (1, 2)))(block.getComparisons())
    assert(12.0 == block.getComparisonSize())
    assertResult(Array(1, 9, 4, 5, 2, 3, 7))(block.getAllProfiles)
    assert("bigdata_111" == block.blockingKey)
    assert(0 == block.blockID)
    assert(0.8 == block.entropy)
  }
  it should " create clean block 2 " in {
    val block = BlockClean(1, Array[Set[Int]](Set[Int](3, 2, 1)), 0.9, 1, "tech_222")
    assertResult(Set())(block.getComparisons())
    assert(0.0 == block.getComparisonSize())
    assertResult(Array[Int](3, 2, 1))(block.getAllProfiles)
    assert("tech_222" == block.blockingKey)
    assert(1 == block.blockID)
    assert(0.9 == block.entropy)
  }
}
