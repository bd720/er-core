package org.bd720.ercore.dataloader
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bd720.ercore.methods.datastructure.{KeyValue, MatchingEntities, Profile}
object SerializedObjectLoader extends WrapperTrait {
  def loadProfiles(filePath: String, startIDFrom: Int = 0, realFieldID: String = "", sourceId: Int = 0): RDD[Profile] = {
    @transient lazy val log = org.apache.log4j.LogManager.getRootLogger
    log.info("SPARKER - Start to loading entities")
    val entities = SerializedLoader.loadSerializedDataset(filePath)
    log.info("SPARKER - Loading ended")
    log.info("SPARKER - Start to generate profiles")
    val profiles: Array[Profile] = new Array(entities.size())
    for (i <- 0 until entities.size()) {
      val profile = Profile(id = i + startIDFrom, originalID = i + "", sourceId = sourceId)
      val entity = entities.get(i)
      val it = entity.attributes.iterator()
      while (it.hasNext) {
        val attribute = it.next()
        profile.addAttribute(KeyValue(attribute.name, attribute.value))
      }
      profiles.update(i, profile)
    }
    log.info("SPARKER - Ended to loading profiles")
    log.info("SPARKER - Start to parallelize profiles")
    val sc = SparkContext.getOrCreate()
    sc.union(profiles.grouped(10000).map(sc.parallelize(_)).toArray)
  }
  def loadGroundtruth(filePath: String): RDD[MatchingEntities] = {
    val groundtruth = SerializedLoader.loadSerializedGroundtruth(filePath)
    val matchingEntitites: Array[MatchingEntities] = new Array(groundtruth.size())
    var i = 0
    val it = groundtruth.iterator
    while (it.hasNext) {
      val matching = it.next()
      matchingEntitites.update(i, MatchingEntities(matching.entityId1.toString, matching.entityId2.toString))
      i += 1
    }
    val sc = SparkContext.getOrCreate()
    sc.union(matchingEntitites.grouped(10000).map(sc.parallelize(_)).toArray)
  }
}
