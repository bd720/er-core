package org.wumiguo.ser.dataloader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.wumiguo.ser.dataloader.JSONWrapper.parseData
import org.wumiguo.ser.methods.datastructure.{MatchingEntities, Profile}
object ParquetWrapper extends WrapperTrait {
  override def loadProfiles(filePath: String, startIDFrom: Int = 0, realIDField: String, sourceId: Int = 0): RDD[Profile] = {
    loadProfiles2(filePath, startIDFrom, realIDField = realIDField, sourceId = sourceId)
  }
  def loadProfiles2(filePath: String, startIDFrom: Int = 0, separator: String = ",", realIDField: String = "-1", sourceId: Int = 0): RDD[Profile] = {
    val sparkSession = SparkSession.builder().getOrCreate()
    val df = sparkSession.read.parquet(filePath)
    df.rdd.zipWithIndex().map { case (row, id) =>
      val theId = realIDField.toInt
      val realID = {
        if (theId == -1) {
          ""
        }
        else {
          row.get(theId).toString
        }
      }
      val p = Profile(id.toInt + startIDFrom, originalID = realID, sourceId = sourceId)
      for (i <- 0 until row.length) {
        if (i != theId) {
          parseData(i.toString, row.get(i), p, Nil, realIDField)
        }
      }
      p
    }
  }
  override def loadGroundtruth(filePath: String): RDD[MatchingEntities] = ???
}
