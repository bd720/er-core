package org.bd720.ercore.dataloader.filter
import org.bd720.ercore.methods.datastructure.KeyValue
trait FieldFilter extends Serializable {
  def filter(kvList: List[KeyValue], fieldValuesScope: List[KeyValue]): Boolean;
}
