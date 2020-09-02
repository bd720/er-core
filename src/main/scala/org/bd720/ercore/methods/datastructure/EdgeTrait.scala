package org.bd720.ercore.methods.datastructure
trait EdgeTrait {
  /* First profile ID */
  val firstProfileID : Int
  val secondProfileID : Int
  def getEntityMatch(map: Map[Int, String]): MatchingEntities = MatchingEntities(map(firstProfileID),map(secondProfileID))
}
