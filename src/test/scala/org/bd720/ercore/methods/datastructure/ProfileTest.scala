package org.wumiguo.ser.methods.datastructure
import org.scalatest.FlatSpec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
class ProfileTest extends FlatSpec {
  it should "" in {
    val attrs = new mutable.MutableList[KeyValue]()
    val originalId = "001"
    val id = 1
    val sourceId = 2
    val profile = Profile(id, attrs, originalId, sourceId)
    assert(profile.id == 1)
    assert(profile.attributes.size == 0)
    val kv = KeyValue("title", "Replication: DB2, Oracle, or Sybase?")
    profile.addAttribute(kv)
    assert(profile.id == 1)
    assert(profile.attributes.size == 1)
    assert(profile.attributes.head == kv)
  }
}
