package org.bd720.ercore.common
import scala.beans.BeanProperty
import scala.collection.mutable
class SparkAppConfiguration {
  @BeanProperty var master: String = ""
  @BeanProperty var enableHiveSupport: Boolean = false
  @BeanProperty var options: mutable.Map[String, String] = mutable.Map()
}
