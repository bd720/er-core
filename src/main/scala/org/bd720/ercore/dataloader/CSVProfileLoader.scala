package org.bd720.ercore.dataloader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.bd720.ercore.dataloader.filter.{DummyFieldFilter, FieldFilter}
import org.bd720.ercore.methods.datastructure
import org.bd720.ercore.methods.datastructure.{KeyValue, MatchingEntities, Profile}
object CSVProfileLoader extends ProfileLoaderTrait {
  override def load(filePath: String, startIDFrom: Int, realIDField: String, sourceId: Int,
                    fieldsToKeep: List[String], keepRealID: Boolean = false,
                    filter: FieldFilter = DummyFieldFilter,
                    fieldValuesScope: List[KeyValue] = Nil): RDD[Profile] = {
    loadProfilesAdvanceMode(filePath, startIDFrom, realIDField = realIDField,
      sourceId = sourceId, header = true,
      fieldsToKeep = fieldsToKeep, keepRealID = keepRealID,
      filter = filter, fieldValuesScope = fieldValuesScope
    )
  }
  def loadProfiles(filePath: String, startIDFrom: Int, realIDField: String, sourceId: Int = 0): RDD[Profile] = {
    loadProfilesAdvanceMode(filePath, startIDFrom, realIDField = realIDField, sourceId = sourceId)
  }
  def loadProfilesAdvanceMode(filePath: String, startIDFrom: Int = 0, separator: String = ",",
                              header: Boolean = false, explodeInnerFields: Boolean = false, innerSeparator: String = ",",
                              realIDField: String = "", sourceId: Int = 0, fieldsToKeep: List[String] = Nil,
                              keepRealID: Boolean = false, filter: FieldFilter = DummyFieldFilter, fieldValuesScope: List[KeyValue] = Nil
                             ): RDD[Profile] = {
    val sparkSession = SparkSession.builder().getOrCreate()
    val df = sparkSession.read.option("header", header).option("sep", separator).option("delimiter", "\"").csv(filePath)
    val columnNames = df.columns
    val lcRealIDField = realIDField.toLowerCase
    df.rdd.map(row => rowToAttributes(columnNames, row, explodeInnerFields, innerSeparator))
      .filter(kvList => filter.filter(kvList.toList, fieldValuesScope))
      .zipWithIndex().map {
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
        Profile(profileID,
          attributes.filter(kv => {
            val lcKey = kv.key.toLowerCase
            (keepRealID && lcKey == lcRealIDField) ||
              ((lcKey != lcRealIDField) &&
                (fieldsToKeep.isEmpty || fieldsToKeep.contains(kv.key)))
          }),
          realID, sourceId)
    }
  }
}
