package org.bd720.ercore.dataloader.filter
import org.bd720.ercore.methods.datastructure.KeyValue
object DummyFieldFilter extends FieldFilter {
  override def filter(kvList: List[KeyValue], fieldValuesScope: List[KeyValue]): Boolean = true
}
