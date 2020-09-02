package org.bd720.ercore.methods.entityclustering
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory
import org.bd720.ercore.methods.datastructure.WeightedEdge
import scala.annotation.tailrec
import scala.collection.mutable
object ConnectedComponents extends Serializable {
  private def smallStar(nodePairs: RDD[(Long, Long)]): (RDD[(Long, Long)], Int) = {
    val neighbors = nodePairs.map(x => {
      val (self, neighbor) = (x._1, x._2)
      if (self > neighbor)
        (self, neighbor)
      else
        (neighbor, self)
    })
    val empty = mutable.HashSet[Long]()
    val allNeighbors = neighbors.aggregateByKey(empty)(
      (lb, v) => lb += v,
      (lb1, lb2) => lb1 ++ lb2
    )
    val newNodePairsWithChangeCount = allNeighbors.map(x => {
      val self = x._1
      val neighbors = x._2.toList
      val minNode = argMin(self :: neighbors)
      val newNodePairs = (self :: neighbors).map(neighbor => {
        (neighbor, minNode)
      }).filter(x => {
        val neighbor = x._1
        val minNode = x._2
        (neighbor <= self && neighbor != minNode) || (self == neighbor)
      })
      val uniqueNewNodePairs = newNodePairs.toSet.toList
      val connectivityChangeCount = (uniqueNewNodePairs diff neighbors.map((self, _))).length
      (uniqueNewNodePairs, connectivityChangeCount)
    }).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val totalConnectivityCountChange = newNodePairsWithChangeCount.mapPartitions(iter => {
      val (v, l) = iter.toSeq.unzip
      val sum = l.foldLeft(0)(_ + _)
      Iterator(sum)
    }).sum.toInt
    val newNodePairs = newNodePairsWithChangeCount.map(x => x._1).flatMap(x => x)
    newNodePairsWithChangeCount.unpersist(false)
    (newNodePairs, totalConnectivityCountChange)
  }
  private def largeStar(nodePairs: RDD[(Long, Long)]): (RDD[(Long, Long)], Int) = {
    val neighbors = nodePairs.flatMap(x => {
      val (self, neighbor) = (x._1, x._2)
      if (self == neighbor)
        List((self, neighbor))
      else
        List((self, neighbor), (neighbor, self))
    })
    val localAdd = (s: mutable.HashSet[Long], v: Long) => s += v
    val partitionAdd = (s1: mutable.HashSet[Long], s2: mutable.HashSet[Long]) => s1 ++= s2
    val allNeighbors = neighbors.aggregateByKey(mutable.HashSet.empty[Long] /*, rangePartitioner*/)(localAdd, partitionAdd)
    val newNodePairsWithChangeCount = allNeighbors.map(x => {
      val self = x._1
      val neighbors: List[Long] = x._2.toList
      val minNode = argMin(self :: neighbors)
      val newNodePairs = (self :: neighbors).map(neighbor => {
        (neighbor, minNode)
      }).filter(x => {
        val neighbor = x._1
        val minNode = x._2
        neighbor >= self
      })
      val uniqueNewNodePairs = newNodePairs.toSet.toList
      val connectivityChangeCount = (uniqueNewNodePairs diff neighbors.map((self, _))).length
      (uniqueNewNodePairs, connectivityChangeCount)
    }).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val totalConnectivityCountChange = newNodePairsWithChangeCount.mapPartitions(iter => {
      val (v, l) = iter.toSeq.unzip
      val sum = l.foldLeft(0)(_ + _)
      Iterator(sum)
    }).sum.toInt
    val newNodePairs = newNodePairsWithChangeCount.map(x => x._1).flatMap(x => x)
    newNodePairsWithChangeCount.unpersist(false)
    (newNodePairs, totalConnectivityCountChange)
  }
  private def argMin(nodes: List[Long]): Long = {
    nodes.min(Ordering.by((node: Long) => node))
  }
  private def buildPairs(nodes: List[Long]): List[(Long, Long)] = {
    buildPairs(nodes.head, nodes.tail, null.asInstanceOf[List[(Long, Long)]])
  }
  @tailrec
  private def buildPairs(node: Long, neighbors: List[Long], partialPairs: List[(Long, Long)]): List[(Long, Long)] = {
    if (neighbors.isEmpty) {
      if (partialPairs != null)
        List((node, node)) ::: partialPairs
      else
        List((node, node))
    } else if (neighbors.length == 1) {
      val neighbor = neighbors(0)
      if (node > neighbor)
        if (partialPairs != null) List((node, neighbor)) ::: partialPairs else List((node, neighbor))
      else if (partialPairs != null) List((neighbor, node)) ::: partialPairs else List((neighbor, node))
    } else {
      val newPartialPairs = neighbors.map(neighbor => {
        if (node > neighbor)
          List((node, neighbor))
        else
          List((neighbor, node))
      }).flatMap(x => x)
      if (partialPairs != null)
        buildPairs(neighbors.head, neighbors.tail, newPartialPairs ::: partialPairs)
      else
        buildPairs(neighbors.head, neighbors.tail, newPartialPairs)
    }
  }
  @tailrec
  private def alternatingAlgo(nodePairs: RDD[(Long, Long)],
                              largeStarConnectivityChangeCount: Int, smallStarConnectivityChangeCount: Int, didConverge: Boolean,
                              currIterationCount: Int, maxIterationCount: Int): (RDD[(Long, Long)], Boolean, Int) = {
    val iterationCount = currIterationCount + 1
    if (didConverge)
      (nodePairs, true, currIterationCount)
    else if (currIterationCount >= maxIterationCount) {
      (nodePairs, false, currIterationCount)
    }
    else {
      val (nodePairsLargeStar, currLargeStarConnectivityChangeCount) = largeStar(nodePairs)
      val (nodePairsSmallStar, currSmallStarConnectivityChangeCount) = smallStar(nodePairsLargeStar)
      if ((currLargeStarConnectivityChangeCount == largeStarConnectivityChangeCount &&
        currSmallStarConnectivityChangeCount == smallStarConnectivityChangeCount) ||
        (currSmallStarConnectivityChangeCount == 0 && currLargeStarConnectivityChangeCount == 0)) {
        alternatingAlgo(nodePairsSmallStar, currLargeStarConnectivityChangeCount,
          currSmallStarConnectivityChangeCount, true, iterationCount, maxIterationCount)
      }
      else {
        alternatingAlgo(nodePairsSmallStar, currLargeStarConnectivityChangeCount,
          currSmallStarConnectivityChangeCount, false, iterationCount, maxIterationCount)
      }
    }
  }
  def run(edges: RDD[WeightedEdge], maxIterationCount: Int): (RDD[(Long, Long)], Boolean, Int) = {
    val (cc, didConverge, iterCount) = alternatingAlgo(edges.map(x => (x.firstProfileID, x.secondProfileID)), 9999999, 9999999, false, 0, maxIterationCount)
    if (didConverge) {
      (cc, didConverge, iterCount)
    } else {
      (null.asInstanceOf[RDD[(Long, Long)]], didConverge, iterCount)
    }
  }
}
