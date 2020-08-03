package org.wumiguo.ser.methods.datastructure
import org.scalatest.FlatSpec
class BlockDirtyTest extends FlatSpec {
  it should " create dirty block 1 " in {
    val block = BlockDirty(0, Array[Set[Int]](Set[Int](1, 9, 4, 5), Set[Int](2, 3, 7)), 0.8, 1, "bigdata_111")
    assertResult(Set((1, 5), (5, 9), (1, 9), (4, 5), (1, 4), (4, 9)))(block.getComparisons())
    assert(12.0 == block.getComparisonSize()) //=4*3
    assertResult(Array(1, 9, 4, 5, 2, 3, 7))(block.getAllProfiles)
    assert("bigdata_111" == block.blockingKey)
    assert(0 == block.blockID)
    assert(0.8 == block.entropy)
  }
  it should " create dirty block 2 " in {
    val block = BlockDirty(1, Array[Set[Int]](Set[Int](3, 2, 1)), 0.9, 1, "tech_222")
    assertResult(Set((2, 3), (1, 3), (1, 2)))(block.getComparisons()) //=3*2
    assert(6.0 == block.getComparisonSize())
    assertResult(Array[Int](3, 2, 1))(block.getAllProfiles)
    assert("tech_222" == block.blockingKey)
    assert(1 == block.blockID)
    assert(0.9 == block.entropy)
  }
}
