package org.bd720.ercore.dataloader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.LoggerFactory
import org.bd720.ercore.dataloader.filter.{DummyFieldFilter, FieldFilter}
import org.bd720.ercore.methods.datastructure.{KeyValue, Profile}
object ParquetProfileLoader extends ProfileLoaderTrait {
  val log = LoggerFactory.getLogger(getClass.getName)
  override def load(filePath: String, startIDFrom: Int, realIDField: String,
                    sourceId: Int, fieldsToKeep: List[String], withReadID: Boolean = false,
                    filter: FieldFilter = DummyFieldFilter,
                    fieldValuesScope: List[KeyValue] = Nil): RDD[Profile] = {
    loadProfilesAdvanceMode(filePath, startIDFrom,
      realIDField = realIDField, sourceId = sourceId,
      fieldsToKeep = fieldsToKeep, keepRealID = withReadID,
      filter = filter,
      fieldValuesScope = fieldValuesScope)
  }
  def loadProfilesAdvanceMode(filePath: String, startIDFrom: Int = 0,
                              realIDField: String = "", sourceId: Int = 0, fieldsToKeep: List[String] = Nil,
                              keepRealID: Boolean = false, explodeInnerFields: Boolean = false, innerSeparator: String = ",",
                              filter: FieldFilter = DummyFieldFilter, fieldValuesScope: List[KeyValue] = Nil): RDD[Profile] = {
    val sparkSession = SparkSession.builder().getOrCreate()
    val df = sparkSession.read.parquet(filePath)
    val columnNames = df.schema.fields.map(_.name)
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
        val p = Profile(profileID, originalID = realID, sourceId = sourceId)
        p.attributes ++= attributes.filter(kv => {
          val lcKey = kv.key.toLowerCase
          (keepRealID && lcKey == lcRealIDField) ||
            ((lcKey != lcRealIDField) &&
              (fieldsToKeep.isEmpty || fieldsToKeep.contains(kv.key)))
        })
        p
    }
  }
}
