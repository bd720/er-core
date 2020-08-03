package org.wumiguo.ser.methods.similarityjoins.common.ed
import org.scalatest.FlatSpec
class EdFiltersTest extends FlatSpec {
  it should "getPrefixLen " in {
    val len = EdFilters.getPrefixLen(2, 2)
    assertResult(2 * 2 + 1)(len)
  }
}
