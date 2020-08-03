package org.wumiguo.ser.dataloader
import org.scalatest.{FlatSpec, FunSuite}
import org.wumiguo.ser.methods.datastructure.KeyValue
class String2ProfileTest extends FunSuite {
  test("parse string to profile") {
    val string = "id=1|originalID=100|sourceId=22|attrs=[id=304586;title=The WASA2 object-oriented workflow management system;author=Gottfried vossen, Mathias Weske;year=1999]"
    val p = String2Profile.string2Profile(string)
    assertResult(1)(p.id)
    assertResult("100")(p.originalID)
    assertResult(22)(p.sourceId)
    assertResult(4)(p.attributes.size)
    val first = p.attributes.sortBy(_.key)
    assertResult(KeyValue("author","Gottfried vossen, Mathias Weske"))(first.head)
  }
}
