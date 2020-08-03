package org.wumiguo.ser.dataloader
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json.{JSONArray, JSONObject}
import org.wumiguo.ser.methods.datastructure.{KeyValue, Profile}
object JSONProfileLoader extends ProfileLoaderTrait {
  def addAttribute(key: String, data: Any, p: Profile): Unit = {
    data match {
      case jsonArray: JSONArray =>
        val it = jsonArray.iterator()
        while (it.hasNext) {
          addAttribute(key, it.next(), p)
        }
      case jsonObject: JSONObject =>
        val it = jsonObject.keys()
        while (it.hasNext) {
          val key = it.next()
          addAttribute(key, jsonObject.get(key), p)
        }
      case _ => p.addAttribute(KeyValue(key, data.toString))
    }
  }
  override def load(filePath: String, startIDFrom: Int = 0, realIDField: String = "", sourceId: Int = 0,
                    fieldsToKeep: List[String] = Nil, keepRealID: Boolean = false): RDD[Profile] = {
    val sc = SparkContext.getOrCreate()
    val raw = sc.textFile(filePath, sc.defaultParallelism)
    raw.zipWithIndex().map { case (row, id) =>
      val obj = new JSONObject(row)
      val realID = {
        if (realIDField.isEmpty) {
          ""
        }
        else {
          obj.get(realIDField).toString
        }
      }
      val p = Profile(id.toInt + startIDFrom, originalID = realID, sourceId = sourceId)
      val keys = obj.keys()
      while (keys.hasNext) {
        val key = keys.next()
        if ((keepRealID && key == realIDField) ||
          (key != realIDField && (fieldsToKeep.isEmpty || fieldsToKeep.contains(key)))) {
          val data = obj.get(key)
          addAttribute(key, data, p)
        }
      }
      p
    }
  }
}
