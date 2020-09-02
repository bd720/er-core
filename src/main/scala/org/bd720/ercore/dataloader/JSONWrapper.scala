package org.bd720.ercore.dataloader
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json.{JSONArray, JSONObject}
import org.bd720.ercore.methods.datastructure.{KeyValue, MatchingEntities, Profile}
object JSONWrapper {
  def parseData(key: String, data: Any, p: Profile, fieldsToKeep: List[String], realIDField: String): Unit = {
    data match {
      case jsonArray: JSONArray =>
        val it = jsonArray.iterator()
        while (it.hasNext) {
          parseData(key, it.next(), p, fieldsToKeep, realIDField)
        }
      case jsonbject: JSONObject =>
        val it = jsonbject.keys()
        while (it.hasNext) {
          val key = it.next()
          parseData(key, jsonbject.get(key), p, fieldsToKeep, realIDField)
        }
      case _ => p.addAttribute(KeyValue(key, data.toString))
    }
  }
  def loadProfiles(filePath: String, startIDFrom: Int = 0, realIDField: String = "", sourceId: Int = 0, fieldsToKeep: List[String] = Nil): RDD[Profile] = {
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
        if (key != realIDField && (fieldsToKeep.isEmpty || fieldsToKeep.contains(key))) {
          val data = obj.get(key)
          parseData(key, data, p, fieldsToKeep, realIDField)
        }
      }
      p
    }
  }
  def loadGroundtruth(filePath: String, firstDatasetAttribute: String, secondDatasetAttribute: String): RDD[MatchingEntities] = {
    val sc = SparkContext.getOrCreate()
    val raw = sc.textFile(filePath)
    raw.map { row =>
      val obj = new JSONObject(row)
      MatchingEntities(obj.get(firstDatasetAttribute).toString, obj.get(secondDatasetAttribute).toString)
    }
  }
}
