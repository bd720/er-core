package org.bd720.ercore.methods.datastructure
trait ProfileTrait {
  val id: Int
  val attributes: scala.collection.mutable.MutableList[KeyValue]
  val originalID: String
  val sourceId: Int
  def getAttributeValues(key: String, separator: String = " "): String = {
    attributes.filter(_.key.equals(key)).map(_.value).mkString(separator)
  }
}
