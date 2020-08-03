package org.wumiguo.ser.dataloader
import org.wumiguo.ser.methods.datastructure.{KeyValue, Profile}
import scala.collection.mutable
object String2Profile extends Serializable {
  def string2Profile(x: String): Profile = {
    val data = x.split("\\|")
    var id = -1
    var originalId = ""
    var sourceId = -1
    var attrs: String = ""
    data.foreach(x => {
      if (x.startsWith("id=")) {
        id = Integer.parseInt(x.substring("id=".length))
      } else if (x.startsWith("originalID=")) {
        originalId = x.substring("originalID=".length)
      } else if (x.startsWith("sourceId=")) {
        sourceId = Integer.parseInt(x.substring("sourceId=".length))
      } else if (x.startsWith("attrs=")) {
        attrs = x.substring("attrs=".length)
        if (attrs.startsWith("[") && attrs.endsWith("]")) {
          attrs = attrs.substring(1, attrs.length - 1)
        } else {
          throw new RuntimeException("[attrs] key-value must be quoted in side '[' and ']' e.g. [key=100;value=100] ")
        }
      }
    })
    if ("".eq(attrs)) {
      throw new RuntimeException("No attributes[attrs]")
    }
    val attrStrings = attrs.split(";").toList
    var attributes = mutable.MutableList[KeyValue]()
    val p = Profile(id, attributes, originalId, sourceId)
    attrStrings.foreach(x => {
      var data = x.split("=")
      val key: String = data(0)
      var value: String = data(1)
      p.addAttribute(KeyValue(key, value))
    })
    p
  }
}
