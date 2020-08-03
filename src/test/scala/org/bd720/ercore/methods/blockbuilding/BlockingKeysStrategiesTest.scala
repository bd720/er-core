package org.wumiguo.ser.methods.blockbuilding
import org.scalatest.FlatSpec
import org.wumiguo.ser.methods.datastructure.KeyValue
import scala.collection.mutable
class BlockingKeysStrategiesTest extends FlatSpec {
  it should " createKeysFromProfileAttributes " in {
    val attrs = new mutable.MutableList[KeyValue]()
    attrs += KeyValue("title", "Hello this is a key")
    attrs += KeyValue("day", "16")
    attrs += KeyValue("month", "06")
    attrs += KeyValue("year", "2020")
    val keysToExc = new mutable.MutableList[String]()
    keysToExc += "month"
    val tokens = BlockingKeysStrategies.createKeysFromProfileAttributes(attrs, keysToExc)
    println(tokens)
    val sorted = tokens.toList.sorted(Ordering.String)
    assertResult(List("16", "2020", "a", "hello", "is", "key", "this"))(sorted)
    assertResult("16")(sorted.head)
  }
}
