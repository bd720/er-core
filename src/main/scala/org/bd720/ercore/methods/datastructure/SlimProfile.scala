package org.bd720.ercore.methods.datastructure
case class SlimProfile(id: Int, attributes: scala.collection.mutable.MutableList[KeyValue] = new scala.collection.mutable.MutableList(), originalID: String = "", sourceId: Int = 0)
  extends SlimProfileTrait with Serializable {
}
  object  SlimProfile{
  def apply(profile : Profile): SlimProfile = {
    SlimProfile(profile.id.toInt, profile.attributes, profile.originalID, profile.sourceId)
  }
}
