package org.wumiguo.ser.dataloader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.wumiguo.ser.dataloader.JSONWrapper.parseData
import org.wumiguo.ser.methods.datastructure.Profile
object ParquetProfileLoader extends ProfileLoaderTrait {
  override def load(filePath: String, startIDFrom: Int, realIDField: String, sourceId: Int, fieldsToKeep: List[String], withReadID: Boolean = false): RDD[Profile] = {
    loadProfilesAdvanceMode(filePath, startIDFrom, realIDField = realIDField, sourceId = sourceId)
  }
  def loadProfilesAdvanceMode(filePath: String, startIDFrom: Int = 0, separator: String = ",", realIDField: String = "-1", sourceId: Int = 0): RDD[Profile] = {
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
}
