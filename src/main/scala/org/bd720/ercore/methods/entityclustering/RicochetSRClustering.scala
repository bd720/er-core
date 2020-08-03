package org.bd720.ercore.methods.entityclustering
import org.apache.spark.rdd.RDD
import EntityClusterUtils.{addUnclusteredProfiles, connectedComponents}
import org.bd720.ercore.methods.datastructure.{Profile, VertexWeight, WeightedEdge}
/*
object RicochetSRClustering extends EntityClusteringTrait {
  override def getClusters(profiles: RDD[Profile], edges: RDD[WeightedEdge], maxProfileID: Int, edgesThreshold: Double, separatorID: Int): RDD[(Int, Set[Int])] = {
    val cc = connectedComponents(edges.filter(_.weight > edgesThreshold))
    val res = cc.mapPartitions { partition =>
      val edgesWeight = Array.fill[Double](maxProfileID + 1) {
        0.0
      }
      val edgesAttached = Array.fill[Int](maxProfileID + 1) {
        0
      }
      val connections = Array.fill[List[(Int, Double)]](maxProfileID + 1) {
        Nil
      }
      val elements = Array.ofDim[Int](maxProfileID + 1)
      var numElements = 0
      /* --------------------------------------------------------------------- */
      /* Generated clusters */
      val clusters = scala.collection.mutable.Map[Int, scala.collection.mutable.HashSet[Int]]()
      val simWithCenter = Array.fill[Double](maxProfileID + 1) {
        0
      }
      val currentCenter = Array.fill[Int](maxProfileID + 1) {
        -1
      }
      val isCenter = Array.fill[Boolean](maxProfileID + 1) {
        false
      }
      val isNonCenter = Array.fill[Boolean](maxProfileID + 1) {
        false
      }
      val centersToReassign = new scala.collection.mutable.HashSet[Int]
      partition.foreach { cluster =>
        cluster.foreach { case (u1, v1a, w) =>
          val u = u1.toInt
          val v = v1a.toInt
          if (edgesWeight(u) == 0) {
            elements.update(numElements, u)
            numElements += 1
          }
          if (edgesWeight(v) == 0) {
            elements.update(numElements, v)
            numElements += 1
          }
          edgesWeight.update(u, edgesWeight(u) + w)
          edgesWeight.update(v, edgesWeight(v) + w)
          edgesAttached.update(u, edgesAttached(u) + 1)
          edgesAttached.update(v, edgesAttached(v) + 1)
          connections.update(u, (v, w) :: connections(u))
          connections.update(v, (u, w) :: connections(v))
        }
        val vv = for (i <- 0 until numElements) yield {
          val e = elements(i)
          VertexWeight(e, edgesWeight(e), edgesAttached(e), connections(e).toMap)
        }
        numElements = 0
        val sorted = vv.toList.sorted
        val v1 = sorted.head.profileId
        currentCenter.update(v1, v1)
        clusters.put(v1, new scala.collection.mutable.HashSet[Int])
        clusters(v1).add(v1)
        isCenter.update(v1, true)
        simWithCenter.update(v1, 1.0)
        sorted.head.connections.foreach { case (v2, v1V2Sim) =>
          isNonCenter.update(v2, true)
          currentCenter.update(v2, v1)
          simWithCenter.update(v2, v1V2Sim)
          clusters(v1).add(v2)
        }
        sorted.tail.foreach { p =>
          val v1 = p.profileId
          val v1Connections = p.connections
          val toReassign = new scala.collection.mutable.HashSet[Int]
          centersToReassign.clear()
          v1Connections.foreach { case (v2, v1V2Sim) =>
            if (!isCenter(v2) && v1V2Sim > simWithCenter(v2)) {
              toReassign.add(v2)
            }
          }
          if (toReassign.nonEmpty) {
            if (isNonCenter(v1)) {
              isNonCenter.update(v1, false)
              val prevV1Center = currentCenter(v1)
              clusters(prevV1Center).remove(v1)
              if (clusters(prevV1Center).size < 2) {
                centersToReassign.add(prevV1Center)
              }
            }
            toReassign.add(v1)
            clusters.put(v1, toReassign)
            isCenter.update(v1, true)
          }
          toReassign.foreach { v2 =>
            if (v2 != v1) {
              if (isNonCenter(v2)) {
                val prevClusterCenter = currentCenter(v2)
                clusters(prevClusterCenter).remove(v2)
                if (clusters(prevClustCenter).size < 2) {
                  centersToReassign.add(prevClusterCenter)
                }
              }
              isNonCenter.update(v2, true)
              currentCenter.update(v2, v1)
              simWithCenter.update(v2, v1Connections(v2))
            }
          }
          centersToReassign.foreach { centerId =>
            if (clusters.contains(centerId))
              if (clusters(centerId).size < 2) {
                isCenter.update(centerId, false)
                clusters.remove(centerId)
                var max = 0.0
                var newCenter = v1
                clusters.keySet.foreach { center =>
                  val currentConnections = connections(center).toMap
                  val newSim = currentConnections.getOrElse(centerId, 0.0)
                  if (newSim > max) {
                    max = newSim
                    newCenter = center
                  }
                }
                clusters(newCenter).add(centerId)
                isNonCenter.update(centerId, true)
                currentCenter.update(centerId, newCenter)
                simWithCenter.update(centerId, max)
              }
          }
        }
      }
      clusters.map(x => (x._1, x._2.toSet)).toIterator
    }
    addUnclusteredProfiles(profiles, res)
  }
}
