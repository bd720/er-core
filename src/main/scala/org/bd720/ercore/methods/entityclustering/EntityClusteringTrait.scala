package org.bd720.ercore.methods.entityclustering
import org.apache.spark.rdd.RDD
import org.bd720.ercore.methods.datastructure.{Profile, WeightedEdge}
trait EntityClusteringTrait {
  def getClusters(profiles: RDD[Profile],
                  edges: RDD[WeightedEdge],
                  maxProfileID: Int,
                  edgesThreshold: Double = 0,
                  separatorID: Int = -1
                 ): RDD[(Int, Set[Int])]
}
