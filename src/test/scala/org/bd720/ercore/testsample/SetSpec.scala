package org.wumiguo.ser.testsample
import java.util.NoSuchElementException
import org.scalatest.FlatSpec
class SetSpec extends FlatSpec {
  "An empty Set" should "have size 0" in {
    assert(Set.empty.size == 0)
  }
  it should "produce NoSuchElementException when head is invoked" in {
    try {
      Set.empty.head
    } catch {
      case err: Throwable => assert(err.isInstanceOf[NoSuchElementException])
    }
  }
}