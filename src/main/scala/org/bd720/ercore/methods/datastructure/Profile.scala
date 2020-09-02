package org.bd720.ercore.methods.datastructure
case class Profile(id: Int,
                   attributes: scala.collection.mutable.MutableList[KeyValue] = new scala.collection.mutable.MutableList(),
                   originalID: String = "",
                   sourceId: Int = 0) extends ProfileTrait with Serializable {
  def addAttribute(a: KeyValue): Unit = {
    attributes += a
  }
}
