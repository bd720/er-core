package org.bd720.ercore.methods.entityclustering
import org.apache.spark.rdd.RDD
import EntityClusterUtils.{addUnclusteredProfiles, connectedComponents}
import org.bd720.ercore.methods.datastructure.{Profile, WeightedEdge}
object MergeCenterClustering extends EntityClusteringTrait {
  override def getClusters(profiles: RDD[Profile], edges: RDD[WeightedEdge], maxProfileID: Int, edgesThreshold: Double, separatorID: Int): RDD[(Int, Set[Int])] = {
    val cc = connectedComponents(edges.filter(_.weight > edgesThreshold))
    val res = cc.repartition(1).mapPartitions { partition =>
      val currentAssignedCenter = Array.fill[Int](maxProfileID + 1) {
        -1
      }
      val isNonCenter = Array.fill[Boolean](maxProfileID + 1) {
        false
      }
      val isCenter = Array.fill[Boolean](maxProfileID + 1) {
        false
      }
      val clusters = scala.collection.mutable.Map[Int, Set[Int]]()
      def mergeClusters(cToKeep: Int, cToMerge: Int): Unit = {
        if (cToKeep > 0 && cToMerge > 0 && cToKeep != cToMerge) {
          val cData = clusters.getOrElse(cToMerge, Set())
          clusters.update(cToKeep, clusters(cToKeep) ++ cData)
          cData.foreach(el => currentAssignedCenter.update(el.toInt, cToKeep))
          clusters.remove(cToMerge)
        }
      }
      partition.foreach { cluster =>
        val sorted = cluster.toList.sortBy(x => (-x._3, x._1))
        sorted.foreach { case (u, v, _) =>
          val uIsCenter = isCenter(u.toInt)
          val vIsCenter = isCenter(v.toInt)
          val uIsNonCenter = isNonCenter(u.toInt)
          val vIsNonCenter = isNonCenter(v.toInt)
          if (!(uIsCenter || vIsCenter || uIsNonCenter || vIsNonCenter)) {
            clusters.put(u, Set(u, v))
            currentAssignedCenter.update(u.toInt, u)
            currentAssignedCenter.update(v.toInt, u)
            isCenter.update(u.toInt, true)
            isNonCenter.update(v.toInt, true)
          }
          else if ((uIsCenter && vIsCenter) || (uIsNonCenter && vIsNonCenter)) {}
          else if (uIsCenter) {
            val currentUassignedCluster = currentAssignedCenter(u.toInt)
            clusters.put(currentUassignedCluster, clusters(currentUassignedCluster) + v)
            mergeClusters(currentUassignedCluster, currentAssignedCenter(v.toInt))
            currentAssignedCenter.update(v.toInt, u)
            isNonCenter.update(v.toInt, true)
          }
          else if (vIsCenter) {
            val currentVassignedCluster = currentAssignedCenter(v.toInt)
            clusters.put(currentVassignedCluster, clusters(currentVassignedCluster) + u)
            mergeClusters(currentVassignedCluster, currentAssignedCenter(u.toInt))
            currentAssignedCenter.update(u.toInt, v)
            isNonCenter.update(u.toInt, true)
          }
        }
      }
      clusters.toIterator
    }
    addUnclusteredProfiles(profiles, res)
  }
}
