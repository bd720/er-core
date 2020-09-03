package org.bd720.ercore.methods.entityclustering
import org.apache.spark.rdd.RDD
import EntityClusterUtils.{addUnclusteredProfiles, connectedComponents}
import org.bd720.ercore.methods.datastructure.{Profile, WeightedEdge}
object UniqueMappingClustering extends EntityClusteringTrait {
  override def getClusters(profiles: RDD[Profile], edges: RDD[WeightedEdge], maxProfileID: Int, edgesThreshold: Double, separatorID: Int): RDD[(Int, Set[Int])] = {
    val cc = connectedComponents(edges.filter(_.weight > edgesThreshold))
    val res = cc.mapPartitions { partition =>
      val visited = Array.fill[Boolean](maxProfileID + 1) {
        false
      }
      val clusters = scala.collection.mutable.Map[Int, Set[Int]]()
      partition.foreach { cluster =>
        val sorted = cluster.toList.sortBy(x => (-x._3, x._1))
        sorted.foreach { case (u, v, sim) =>
          if (!visited(u.toInt) && !visited(v.toInt)) {
            visited.update(u.toInt, true)
            visited.update(v.toInt, true)
            clusters.put(u, Set(u, v))
          }
        }
      }
      clusters.toIterator
    }
    addUnclusteredProfiles(profiles, res)
  }
}
