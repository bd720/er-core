package org.bd720.ercore.dataloader.filter
import org.slf4j.LoggerFactory
import org.bd720.ercore.methods.datastructure.KeyValue
object SpecificFieldValueFilter extends FieldFilter {
  val log = LoggerFactory.getLogger(this.getClass.getName)
  override def filter(kvList: List[KeyValue], fieldValuesScope: List[KeyValue]): Boolean = {
    if (fieldValuesScope.map(_.key).toSet.size == fieldValuesScope.size) {
      fieldValuesScope.intersect(kvList) == fieldValuesScope
    } else {
      val kvMap = fieldValuesScope.groupBy(_.key).map(x => (x._1, x._2.map(_.value)))
      val matchItem = kvList.filter(x => kvMap.get(x.key).getOrElse(Nil).contains(x.value)).size
      matchItem == kvMap.size
    }
  }
}
