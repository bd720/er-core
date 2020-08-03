package org.wumiguo.ser.dataloader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.wumiguo.ser.dataloader.CSVWrapper.rowToAttributes
import org.wumiguo.ser.methods.datastructure
import org.wumiguo.ser.methods.datastructure.{MatchingEntities, Profile}
object CSVProfileLoader extends ProfileLoaderTrait {
  def loadProfiles(filePath: String, startIDFrom: Int, realIDField: String, sourceId: Int = 0): RDD[Profile] = {
    loadProfilesAdvanceMode(filePath, startIDFrom, realIDField = realIDField, sourceId = sourceId)
  }
  def loadProfilesAdvanceMode(filePath: String, startIDFrom: Int = 0, separator: String = ",", header: Boolean = false,
                              explodeInnerFields: Boolean = false, innerSeparator: String = ",", realIDField: String = "", sourceId: Int = 0,
                              fieldsToKeep: List[String] = Nil, keepRealID: Boolean = false): RDD[Profile] = {
    val sparkSession = SparkSession.builder().getOrCreate()
    val df = sparkSession.read.option("header", header).option("sep", separator).option("delimiter", "\"").csv(filePath)
    val columnNames = df.columns
    val lcRealIDField = realIDField.toLowerCase
    df.rdd.map(row => rowToAttributes(columnNames, row, explodeInnerFields, innerSeparator)).zipWithIndex().map {
      profile =>
        val profileID = profile._2.toInt + startIDFrom
        val attributes = profile._1
        val realID = {
          if (realIDField.isEmpty) {
            ""
          }
          else {
            attributes.filter(_.key.toLowerCase() == lcRealIDField).map(_.value).mkString("").trim
          }
        }
        datastructure.Profile(profileID,
          attributes.filter(kv => {
            val lcKey = kv.key.toLowerCase
            (keepRealID && lcKey == lcRealIDField) ||
              ((lcKey != lcRealIDField) && (fieldsToKeep.isEmpty || fieldsToKeep.contains(kv.key)))
          }),
          realID, sourceId)
    }
  }
  def loadGroundTruth(filePath: String): RDD[MatchingEntities] = {
    loadGroundTruth(filePath, ",", false)
  }
  def loadGroundTruth(filePath: String, separator: String = ",", header: Boolean = false): RDD[MatchingEntities] = {
    val sparkSession = SparkSession.builder().getOrCreate()
    val df = sparkSession.read.option("header", header).option("sep", separator).csv(filePath)
    df.rdd map {
      row =>
        MatchingEntities(row.get(0).toString, row.get(1).toString)
    }
  }
  override def load(filePath: String, startIDFrom: Int, realIDField: String, sourceId: Int, fieldsToKeep: List[String], keepRealID: Boolean = false): RDD[Profile] = {
    loadProfilesAdvanceMode(filePath, startIDFrom, realIDField = realIDField,
      sourceId = sourceId, header = true, fieldsToKeep = fieldsToKeep, keepRealID = keepRealID)
  }
}
