package org.bd720.ercore.dataloader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.bd720.ercore.methods.datastructure
import org.bd720.ercore.methods.datastructure.{MatchingEntities, Profile}
object CSVWrapper extends WrapperTrait {
  override def loadProfiles(filePath: String, startIDFrom: Int, realIDField: String, sourceId: Int = 0): RDD[Profile] = {
    loadProfiles2(filePath, startIDFrom, realIDField = realIDField, sourceId = sourceId)
  }
  def loadProfiles2(filePath: String, startIDFrom: Int = 0, separator: String = ",", header: Boolean = false,
                   explodeInnerFields: Boolean = false, innerSeparator: String = ",", realIDField: String = "", sourceId: Int = 0): RDD[Profile] = {
    val sparkSession = SparkSession.builder().getOrCreate()
    val df = sparkSession.read.option("header", header).option("sep", separator).option("delimiter", "\"").csv(filePath)
    val columnNames = df.columns
    df.rdd.map(row => rowToAttributes(columnNames, row, explodeInnerFields, innerSeparator)).zipWithIndex().map {
      profile =>
        val profileID = profile._2.toInt + startIDFrom
        val attributes = profile._1
        val realID = {
          if (realIDField.isEmpty) {
            ""
          }
          else {
            attributes.filter(_.key.toLowerCase() == realIDField.toLowerCase()).map(_.value).mkString("").trim
          }
        }
        datastructure.Profile(profileID, attributes.filter(kv => kv.key.toLowerCase() != realIDField.toLowerCase()), realID, sourceId)
    }
  }
  override def loadGroundtruth(filePath: String): RDD[MatchingEntities] = {
    loadGroundtruth(filePath, ",", false)
  }
  def loadGroundtruth(filePath: String, separator: String = ",", header: Boolean = false): RDD[MatchingEntities] = {
    val sparkSession = SparkSession.builder().getOrCreate()
    val df = sparkSession.read.option("header", header).option("sep", separator).csv(filePath)
    df.rdd map {
      row =>
        MatchingEntities(row.get(0).toString, row.get(1).toString)
    }
  }
}
