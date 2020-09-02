package org.bd720.ercore.dataloader.filter
import org.scalatest.flatspec.AnyFlatSpec
import org.bd720.ercore.methods.datastructure.KeyValue
class DummyFieldValueFilterTest extends AnyFlatSpec {
  it should "always filter in" in {
    val kvList = List(
      KeyValue("name", "test"), KeyValue("title", "a dummy filter")
    )
    val fieldValuesScope = List(
      KeyValue("name", "____")
    )
    assert(DummyFieldFilter.filter(kvList, fieldValuesScope))
  }
}
