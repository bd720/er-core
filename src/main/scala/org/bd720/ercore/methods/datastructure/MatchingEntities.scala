package org.bd720.ercore.methods.datastructure
case class MatchingEntities(firstEntityID: String, secondEntityID: String) {
  override def equals(that: Any): Boolean = {
    that match {
      case that: MatchingEntities =>
        that.firstEntityID == this.firstEntityID && that.secondEntityID == this.secondEntityID
      case _ => false
    }
  }
  override def hashCode: Int = {
    val firstEntityHashCode = if (firstEntityID == null) 0 else firstEntityID.hashCode
    val secondEntityHashCode = if (secondEntityID == null) 0 else secondEntityID.hashCode
    (firstEntityHashCode + "_" + secondEntityHashCode).hashCode
  }
}
