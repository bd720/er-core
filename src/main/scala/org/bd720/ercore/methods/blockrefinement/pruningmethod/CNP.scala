package org.bd720.ercore.methods.blockrefinement.pruningmethod
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bd720.ercore.methods.blockrefinement.pruningmethod.PruningUtils.WeightTypes
import org.bd720.ercore.methods.datastructure.{BlockAbstract, ProfileBlocks, UnweightedEdge}
import org.bd720.ercore.methods.util.BoundedPriorityQueue
object CNP {
  def CNP(blocks: RDD[BlockAbstract],
          numberOfProfiles: Int,
          profileBlocksFiltered: RDD[ProfileBlocks],
          blockIndex: Broadcast[scala.collection.Map[Int, Array[Set[Int]]]],
          maxID: Int,
          separatorID: Array[Int],
          groundtruth: Broadcast[scala.collection.immutable.Set[(Int, Int)]],
          thresholdType: String = PruningUtils.ThresholdTypes.AVG,
          weightType: String = WeightTypes.CBS,
          profileBlocksSizeIndex: Broadcast[scala.collection.Map[Int, Int]] = null,
          useEntropy: Boolean = false,
          blocksEntropies: Broadcast[scala.collection.Map[Int, Double]] = null,
          comparisonType: String = PruningUtils.ComparisonTypes.OR
         )
  : RDD[(Double, Double, Iterable[UnweightedEdge])] = {
    if (useEntropy && blocksEntropies == null) {
      throw new Exception("blocksEntropies must be defined")
    }
    if ((weightType == WeightTypes.ECBS || weightType == WeightTypes.EJS || weightType == WeightTypes.JS || weightType == WeightTypes.chiSquare) && profileBlocksSizeIndex == null) {
      throw new Exception("profileBlocksSizeIndex must be defined")
    }
    if (!List(WeightTypes.CBS, WeightTypes.JS, WeightTypes.chiSquare, WeightTypes.ARCS, WeightTypes.ECBS, WeightTypes.EJS).contains(weightType)) {
      throw new Exception("Please provide a valid WeightType, " + weightType + " is not an acceptable value!")
    }
    if (!List(PruningUtils.ComparisonTypes.OR, PruningUtils.ComparisonTypes.AND).contains(comparisonType)) {
      throw new Exception("Please provide a valid ComparisonType, " + comparisonType + " is not an acceptable value!")
    }
    val sc = SparkContext.getOrCreate()
    val CNPThreshold = computeCNPThreshold(blocks, numberOfProfiles)
    var numberOfEdges: Double = 0
    var edgesPerProfile: Broadcast[scala.collection.Map[Int, Double]] = null
    if (weightType == WeightTypes.EJS) {
      val stats = CommonNodePruning.computeStatistics(profileBlocksFiltered, blockIndex, maxID, separatorID)
      numberOfEdges = stats.map(_._1.toDouble).sum()
      edgesPerProfile = sc.broadcast(stats.map(x => (x._2._1, x._2._2.toDouble)).groupByKey().map(x => (x._1, x._2.sum)).collectAsMap())
    }
    val retainedNeighbours = sc.broadcast(calcRetainedNeighbours(profileBlocksFiltered, blockIndex, maxID, separatorID, thresholdType, weightType, profileBlocksSizeIndex, useEntropy, blocksEntropies, numberOfEdges, edgesPerProfile, CNPThreshold).collectAsMap())
    val edges = pruning(profileBlocksFiltered, blockIndex, separatorID, groundtruth, retainedNeighbours, comparisonType, maxID)
    retainedNeighbours.unpersist()
    if (edgesPerProfile != null) {
      edgesPerProfile.unpersist()
    }
    edges
  }
  def computeCNPThreshold(blocks: RDD[BlockAbstract], numberOfProfiles: Long): Int = {
    val numElements = blocks.map(_.getAllProfiles.size).sum()
    Math.floor((numElements / numberOfProfiles) - 1).toInt
  }
  def calcRetainedNeighbours(profileBlocksFiltered: RDD[ProfileBlocks],
                             blockIndex: Broadcast[scala.collection.Map[Int, Array[Set[Int]]]],
                             maxID: Int,
                             separatorID: Array[Int],
                             thresholdType: String,
                             weightType: String,
                             profileBlocksSizeIndex: Broadcast[scala.collection.Map[Int, Int]],
                             useEntropy: Boolean,
                             blocksEntropies: Broadcast[scala.collection.Map[Int, Double]],
                             numberOfEdges: Double,
                             edgesPerProfile: Broadcast[scala.collection.Map[Int, Double]],
                             CNPThreshold: Int
                            ): RDD[(Int, Set[Int])] = {
    profileBlocksFiltered.mapPartitions { partition =>
      val localWeights = Array.fill[Double](maxID + 1) {
        0
      }
      val neighbours = Array.ofDim[Int](maxID + 1)
      var neighboursNumber = 0
      val neighboursToKeep = new BoundedPriorityQueue[NeighbourWithWeight](CNPThreshold)
      val entropies: Array[Double] = {
        if (useEntropy) {
          Array.fill[Double](maxID + 1) {
            0.0
          }
        }
        else {
          null
        }
      }
      partition.map { pb =>
        neighboursNumber = CommonNodePruning.calcCBS(pb, blockIndex, separatorID, useEntropy, blocksEntropies, localWeights, entropies, neighbours, true)
        CommonNodePruning.calcWeights(pb, localWeights, neighbours, entropies, neighboursNumber, blockIndex, separatorID, weightType, profileBlocksSizeIndex, useEntropy, numberOfEdges, edgesPerProfile)
        for (i <- 0 until neighboursNumber) {
          val neighbourID = neighbours(i)
          neighboursToKeep += NeighbourWithWeight(neighbourID, localWeights(neighbourID))
        }
        val sortedNeighbours = neighboursToKeep.toArray.map(_.neighbourID).toSet
        CommonNodePruning.doReset(localWeights, neighbours, entropies, useEntropy, neighboursNumber)
        neighboursToKeep.clear()
        (pb.profileID, sortedNeighbours)
      }
    }
  }
  def pruning(profileBlocksFiltered: RDD[ProfileBlocks],
              blockIndex: Broadcast[scala.collection.Map[Int, Array[Set[Int]]]],
              separatorID: Array[Int],
              groundtruth: Broadcast[scala.collection.immutable.Set[(Int, Int)]],
              retainedNeighbours: Broadcast[scala.collection.Map[Int, Set[Int]]],
              comparisonType: String,
              maxID : Int
             ): RDD[(Double, Double, Iterable[UnweightedEdge])] = {
    profileBlocksFiltered.mapPartitions { partition =>
      val visited = Array.fill[Boolean](maxID + 1) {
        false
      }
      val neighbours = Array.ofDim[Int](maxID + 1)
      var neighboursNumber = 0
      partition.map { pb =>
        val profileID = pb.profileID
        val profileBlocks = pb.blocks
        val profileRetainedNeighbours = retainedNeighbours.value(profileID)
        var cont: Double = 0
        var gtNum: Double = 0
        var edges: List[UnweightedEdge] = Nil
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
              val neighbourID = secondProfileID
              val neighbourRetainedNeighbours = retainedNeighbours.value(neighbourID)
              if (profileID < neighbourID && !visited(neighbourID)) {
                visited.update(neighbourID, true)
                neighbours.update(neighboursNumber, neighbourID)
                neighboursNumber += 1
                if (comparisonType == PruningUtils.ComparisonTypes.OR
                  && (profileRetainedNeighbours.contains(neighbourID) || neighbourRetainedNeighbours.contains(profileID))
                ) {
                  cont += 1
                  if (groundtruth != null && groundtruth.value.contains((profileID, neighbourID))) {
                    gtNum += 1
                  }
                  edges = UnweightedEdge(profileID, neighbourID) :: edges
                }
                else if (comparisonType == PruningUtils.ComparisonTypes.AND && neighbourRetainedNeighbours.contains(profileID) && profileRetainedNeighbours.contains(neighbourID)) {
                  cont += 1
                  if (groundtruth != null && groundtruth.value.contains((profileID, neighbourID))) {
                    gtNum += 1
                  }
                  edges = UnweightedEdge(profileID, neighbourID) :: edges
                }
              }
            }
          }
        }
        for(i <- 0 until neighboursNumber){
          visited.update(neighbours(i), false)
        }
        neighboursNumber = 0
        (cont, gtNum, edges)
      }
    }
  }
  case class NeighbourWithWeight(neighbourID: Int, weight: Double) extends Ordered[NeighbourWithWeight] {
    def compare(that: NeighbourWithWeight): Int = {
      this.weight compare that.weight
    }
  }
}
