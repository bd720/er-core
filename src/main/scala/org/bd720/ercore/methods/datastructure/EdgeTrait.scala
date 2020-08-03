package org.wumiguo.ser.methods.datastructure
trait EdgeTrait {
  /* First profile ID */
  val firstProfileID : Int
  val secondProfileID : Int
  def getEntityMatch(map: Map[Int, String]): MatchingEntities = MatchingEntities(map(firstProfileID),map(secondProfileID))
}
