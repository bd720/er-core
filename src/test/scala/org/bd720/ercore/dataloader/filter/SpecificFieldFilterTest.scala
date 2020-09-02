package org.bd720.ercore.dataloader.filter
import org.scalatest.flatspec.AnyFlatSpec
import org.bd720.ercore.methods.datastructure.KeyValue
class SpecificFieldFilterTest extends AnyFlatSpec {
  it should " filter in 1" in {
    val kvList = List(
      KeyValue("name", "test"), KeyValue("title", "a dummy filter")
    )
    val fieldValuesScope = List(
      KeyValue("name", "test")
    )
    assert(SpecificFieldValueFilter.filter(kvList, fieldValuesScope))
  }
  it should " filter in 2" in {
    val kvList = List(
      KeyValue("name", "test"), KeyValue("title", "a dummy filter")
    )
    val fieldValuesScope = List(
      KeyValue("name", "test"), KeyValue("name", "test1234")
    )
    assert(SpecificFieldValueFilter.filter(kvList, fieldValuesScope))
  }
  it should " filter in 3" in {
    val kvList = List(
      KeyValue("name", "test"), KeyValue("title", "a dummy filter")
    )
    val fieldValuesScope = List(
      KeyValue("name", "test"), KeyValue("name", "test1234"),
      KeyValue("name", "a dummy filter")
    )
    assert(SpecificFieldValueFilter.filter(kvList, fieldValuesScope))
  }
  it should " filter out" in {
    val kvList = List(
      KeyValue("name", "test"), KeyValue("title", "a dummy filter")
    )
    val fieldValuesScope = List(
      KeyValue("name", "")
    )
    assertResult(false)(SpecificFieldValueFilter.filter(kvList, fieldValuesScope))
  }
}
