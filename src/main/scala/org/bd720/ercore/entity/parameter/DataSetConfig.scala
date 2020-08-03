package org.wumiguo.ser.entity.parameter
import scala.beans.BeanProperty
class DataSetConfig(@BeanProperty var path: String, @BeanProperty var format: String, @BeanProperty var dataSetId: String, @BeanProperty var attributes: Array[String]) {
  override def toString: String = s"path: $path, format: $format, dataSetId: $dataSetId, attributes: ${attributes.toList}"
}
