package org.bd720.ercore.methods.blockrefinement.pruningmethod
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bd720.ercore.methods.datastructure.{BlockAbstract, ProfileBlocks, UnweightedEdge}
import org.bd720.ercore.methods.util.Converters
object PCPQBlockCalc {
  def getPcPq2(blocks: RDD[BlockAbstract], newGT: Set[(Int, Int)], maxProfileID: Int, separatorID: Array[Int]): (Float, Float, Double, Long) = {
    val sc = SparkContext.getOrCreate()
    val blockIndexMap1 = blocks.map(b => (b.blockID, b.profiles)).collectAsMap()
    val blockIndex1 = sc.broadcast(blockIndexMap1)
    val gt1 = sc.broadcast(newGT)
    val profileBlocks1 = Converters.blocksToProfileBlocks(blocks)
    val edgesAndCount = getStats(
      profileBlocks1,
      blockIndex1,
      maxProfileID,
      separatorID,
      gt1
    )
    val numEdges = edgesAndCount.map(_._1).sum()
    val edges = edgesAndCount.flatMap(_._2).distinct()
    val perfectMatch = edges.count()
    val newGTSize = newGT.size
    val pc = {
      try {
        perfectMatch.toFloat / newGTSize.toFloat
      }
      catch {
        case _: Exception => 0
      }
    }
    val pq = {
      try {
        perfectMatch.toFloat / numEdges.toFloat
      }
      catch {
        case _: Exception => 0
      }
    }
    gt1.unpersist()
    (pc, pq, numEdges, perfectMatch)
  }
  def getPcPq(blocks: RDD[BlockAbstract], newGT: Set[(Int, Int)], maxProfileID: Int, separatorID: Array[Int]): (Double, Double, Double) = {
    val sc = SparkContext.getOrCreate()
    val blockIndexMap1 = blocks.map(b => (b.blockID, b.profiles)).collectAsMap()
    val blockIndex1 = sc.broadcast(blockIndexMap1)
    val gt1 = sc.broadcast(newGT)
    val profileBlocks1 = Converters.blocksToProfileBlocks(blocks)
    val edgesAndCount = getStats(
      profileBlocks1,
      blockIndex1,
      maxProfileID,
      separatorID,
      gt1
    )
    val numEdges = edgesAndCount.map(_._1).sum()
    val edges = edgesAndCount.flatMap(_._2).distinct()
    val perfectMatch = edges.count()
    val newGTSize = newGT.size
    println("Perfect matches " + perfectMatch)
    println("Missed matches " + (newGTSize - perfectMatch))
    val pc = perfectMatch.toFloat / newGTSize.toFloat
    val pq = perfectMatch.toFloat / numEdges.toFloat
    (pc, pq, numEdges)
  }
  def getStats(profileBlocksFiltered: RDD[ProfileBlocks],
               blockIndex: Broadcast[scala.collection.Map[Int, Array[Set[Int]]]],
               maxID: Int,
               separatorID: Array[Int],
               groundtruth: Broadcast[scala.collection.immutable.Set[(Int, Int)]]
              )
  : RDD[(Double, Iterable[UnweightedEdge])] = {
    pruning(profileBlocksFiltered, blockIndex, maxID, separatorID, groundtruth)
  }
  def doPruning(profileID: Int,
                neighbours: Array[Int],
                neighboursNumber: Int,
                groundtruth: Broadcast[scala.collection.immutable.Set[(Int, Int)]]
               ): (Double, Iterable[UnweightedEdge]) = {
    var edges: List[UnweightedEdge] = Nil
    var cont: Double = 0
    for (i <- 0 until neighboursNumber) {
      val neighbourID = neighbours(i)
      cont += 1
      if (groundtruth.value.contains((profileID, neighbourID))) {
        edges = UnweightedEdge(profileID, neighbours(i)) :: edges
      }
    }
    (cont, edges)
  }
  def pruning(profileBlocksFiltered: RDD[ProfileBlocks],
              blockIndex: Broadcast[scala.collection.Map[Int, Array[Set[Int]]]],
              maxID: Int,
              separatorID: Array[Int],
              groundtruth: Broadcast[scala.collection.immutable.Set[(Int, Int)]]
             ): RDD[(Double, Iterable[UnweightedEdge])] = {
    profileBlocksFiltered.mapPartitions { partition =>
      val localWeights = Array.fill[Double](maxID + 1) {
        0
      }
      val neighbours = Array.ofDim[Int](maxID + 1)
      var neighboursNumber = 0
      partition.map { pb =>
        neighboursNumber = calcNeighbors(pb, blockIndex, separatorID, localWeights, neighbours)
        val result = doPruning(pb.proeID, neighbours, neighboursNumber, groundtruth)
        doReset(localWeights, neighbours, neighboursNumber)
        result
      }
    }
  }
  def doReset(weights: Array[Double],
              neighbours: Array[Int],
              neighboursNumber: Int): Unit = {
    for (i <- 0 until neighboursNumber) {
      val neighbourID = neighbours(i)
      weights.update(neighbourID, 0)
    }
  }
  def calcNeighbors(pb: ProfileBlocks,
                    blockIndex: Broadcast[scala.collection.Map[Int, Array[Set[Int]]]],
                    separatorID: Array[Int],
                    weights: Array[Double],
                    neighbours: Array[Int]
                    ): Int = {
    var neighboursNumber = 0
    val profileID = pb.profileID
    val profileBlocks = pb.blocks
    profileBlocks.foreach { block =>
      val blockID = block.blockID
      val blockProfiles = blockIndex.value.get(blockID)
      if (blockProfiles.isDefined) {
        val profilesIDs = {
          if (separatorID.isEmpty) {
            blockProfiles.get.head
          }
          else {
            PruningUtils.getAllNeighbors(profileID, blockProfiles.get, separatorID)
          }
        }
        profilesIDs.foreach { secondProfileID =>
          val neighbourID = secondProfileID.toInt
          if (profileID < neighbourID) {
            weights.update(neighbourID, weights(neighbourID) + 1)
            if (weights(neighbourID) == 1) {
              neighbours.update(neighboursNumber, neighbourID)
              neighboursNumber += 1
            }
          }
        }
      }
    }
    neighboursNumber
  }
}
