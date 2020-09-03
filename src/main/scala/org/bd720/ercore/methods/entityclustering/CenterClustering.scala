package org.bd720.ercore.methods.entityclustering
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import EntityClusterUtils.{addUnclusteredProfiles, connectedComponents}
import org.bd720.ercore.methods.datastructure.{Profile, WeightedEdge}
object CenterClustering extends EntityClusteringTrait {
  override def getClusters(profiles: RDD[Profile], edges: RDD[WeightedEdge], maxProfileID: Int, edgesThreshold: Double, separatorID: Int): RDD[(Int, Set[Int])] = {
    val filteredEdges = edges.filter(_.weight > edgesThreshold)
    val stats = filteredEdges.flatMap(x => (x.firstProfileID, (x.weight, 1)) :: (x.secondProfileID, (x.weight, 1)) :: Nil).groupByKey().map { x =>
      val edgesWeight = x._2.map(_._1).sum
      val edgesAttached = x._2.map(_._2).sum
      (x._1, edgesWeight / edgesAttached)
    }
    val statsB = SparkContext.getOrCreate().broadcast(stats.collectAsMap()) 
    val cc = connectedComponents(filteredEdges)
    val res = cc.mapPartitions { partition =>
      val isCenter = Array.fill[Boolean](maxProfileID + 1) {
        false
      }
      val isNonCenter = Array.fill[Boolean](maxProfileID + 1) {
        false
      }
      val clusters = scala.collection.mutable.Map[Int, Set[Int]]()
      partition.foreach { cluster =>
        val sorted = cluster.toList.sortBy(x => (-x._3, x._1))
        sorted.foreach { case (u, v, sim) =>
          val uIsCenter = isCenter(u.toInt)
          val vIsCenter = isCenter(v.toInt)
          val uIsNonCenter = isNonCenter(u.toInt)
          val vIsNonCenter = isNonCenter(v.toInt)
          if (!(uIsCenter || vIsCenter || uIsNonCenter || vIsNonCenter)) {
            val w1 = statsB.value.getOrElse(u, 0.0)
            val w2 = statsB.value.getOrElse(v, 0.0)
            if (w1 > w2) {
              clusters.put(u, Set(u, v))
              isCenter.update(u.toInt, true)
              isNonCenter.update(v.toInt, true)
            }
            else {
              clusters.put(v, Set(u, v))
              isCenter.update(v.toInt, true)
              isNonCenter.update(u.toInt, true)
            }
          }
          else if ((uIsCenter && vIsCenter) || (uIsNonCenter && vIsNonCenter)) {}
          else if (uIsCenter && !vIsNonCenter) {
            clusters.put(u, clusters(u.toInt) + v)
            isNonCenter.update(v.toInt, true)
          }
          else if (vIsCenter && !uIsNonCenter) {
            clusters.put(v, clusters(v.toInt) + u)
            isNonCenter.update(u.toInt, true)
          }
        }
      }
      clusters.toIterator
    }
    addUnclusteredProfiles(profiles, res)
  }
}
