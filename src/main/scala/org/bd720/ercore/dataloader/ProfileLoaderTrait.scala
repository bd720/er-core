package org.wumiguo.ser.dataloader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.wumiguo.ser.methods.datastructure.{KeyValue, Profile}
import scala.collection.mutable.MutableList
trait ProfileLoaderTrait {
  def load(filePath: String, startIDFrom: Int = 0, realIDField: String,
           sourceId: Int = 0, fieldsToKeep: List[String] = Nil, keepRealID: Boolean = false): RDD[Profile]
  def rowToAttributes(columnNames: Array[String], row: Row, explodeInnerFields: Boolean = false, innerSeparator: String = ","): MutableList[KeyValue] = {
    val attributes: MutableList[KeyValue] = new MutableList()
    for (i <- 0 until row.size) {
      try {
        val value = row(i)
        val attributeKey = columnNames(i)
        if (value != null) {
          value match {
            case listOfAttributes: Iterable[Any] =>
              listOfAttributes map {
                attributeValue =>
                  attributes += KeyValue(attributeKey, attributeValue.toString)
              }
            case stringAttribute: String =>
              if (explodeInnerFields) {
                stringAttribute.split(innerSeparator) map {
                  attributeValue =>
                    attributes += KeyValue(attributeKey, attributeValue)
                }
              }
              else {
                attributes += KeyValue(attributeKey, stringAttribute)
              }
            case singleAttribute =>
              attributes += KeyValue(attributeKey, singleAttribute.toString)
          }
        }
      }
      catch {
        case e: Throwable => println(e)
      }
    }
    attributes
  }
}
