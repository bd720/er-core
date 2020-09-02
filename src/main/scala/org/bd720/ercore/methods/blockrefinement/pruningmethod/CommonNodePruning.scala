package org.bd720.ercore.methods.blockrefinement.pruningmethod
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bd720.ercore.methods.blockrefinement.pruningmethod.PruningUtils.WeightTypes
import org.bd720.ercore.methods.datastructure.ProfileBlocks
object CommonNodePruning {
  def computeStatistics(profileBlocksFiltered: RDD[ProfileBlocks],
                        blockIndex: Broadcast[scala.collection.Map[Int, Array[Set[Int]]]],
                        maxID: Int,
                        separatorID: Array[Int]
                       ): RDD[(Long, (Int, Int))] = {
    profileBlocksFiltered.mapPartitions { partition =>
      val localWeights = Array.fill[Double](maxID + 1) {
        0
      }
      val neighbours = Array.ofDim[Int](maxID + 1)
      partition.map { pb =>
        var neighboursNumber = 0
        var distinctEdges = 0L
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
              val neighbourID = secondProfileID
              val neighbourWeight = localWeights(neighbourID)
              localWeights.update(neighbourID, neighbourWeight + 1)
              if (neighbourWeight == 1) {//Todo: per me deve essere == 0 non == 1
                neighbours.update(neighboursNumber, neighbourID)
                neighboursNumber += 1
                if (profileID < neighbourID) {
                  distinctEdges += 1
                }
              }
            }
          }
        }
        for (i <- 0 until neighboursNumber) {
          localWeights.update(neighbours(i), 0)
        }
        (distinctEdges, (profileID, neighboursNumber))
      }
    }
  }
  def calcChiSquare(CBS: Double, neighbourNumBlocks: Double, currentProfileNumBlocks: Double, totalNumberOfBlocks: Double): Double = {
    val CMat = Array.ofDim[Double](3, 3)
    var weight: Double = 0
    var expectedValue: Double = 0
    CMat(0)(0) = CBS
    CMat(0)(1) = neighbourNumBlocks - CBS
    CMat(0)(2) = neighbourNumBlocks
    CMat(1)(0) = currentProfileNumBlocks - CBS
    CMat(1)(1) = totalNumberOfBlocks - (neighbourNumBlocks + currentProfileNumBlocks - CBS)
    CMat(1)(2) = totalNumberOfBlocks - neighbourNumBlocks
    CMat(2)(0) = currentProfileNumBlocks
    CMat(2)(1) = totalNumberOfBlocks - currentProfileNumBlocks
    for (i <- 0 to 1) {
      for (j <- 0 to 1) {
        expectedValue = (CMat(i)(2) * CMat(2)(j)) / totalNumberOfBlocks
        weight += Math.pow(CMat(i)(j) - expectedValue, 2) / expectedValue
      }
    }
    weight
  }
  def calcCBS(pb: ProfileBlocks,
              blockIndex: Broadcast[scala.collection.Map[Int, Array[Set[Int]]]],
              separatorID: Array[Int],
              useEntropy: Boolean,
              blocksEntropies: Broadcast[scala.collection.Map[Int, Double]],
              weights: Array[Double],
              entropies: Array[Double],
              neighbours: Array[Int],
              firstStep: Boolean
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
        val blockEntropy = {
          if (useEntropy) {
            val e = blocksEntropies.value.get(blockID)
            if (e.isDefined) {
              e.get
            }
            else {
              0.0
            }
          }
          else {
            0.0
          }
        }
        profilesIDs.foreach { secondProfileID =>
          val neighbourID = secondProfileID.toInt
          if ((profileID < neighbourID) || firstStep) {
            weights.update(neighbourID, weights(neighbourID) + 1)
            if (useEntropy) {
              entropies.update(neighbourID, entropies(neighbourID) + blockEntropy)
            }
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
  def calcWeights(pb: ProfileBlocks,
                  weights: Array[Double],
                  neighbours: Array[Int],
                  entropies: Array[Double],
                  neighboursNumber: Int,
                  blockIndex: Broadcast[scala.collection.Map[Int, Array[Set[Int]]]],
                  separatorID: Array[Int],
                  weightType: String,
                  profileBlocksSizeIndex: Broadcast[scala.collection.Map[Int, Int]],
                  useEntropy: Boolean,
                  numberOfEdges: Double,
                  edgesPerProfile: Broadcast[scala.collection.Map[Int, Double]]
                 ): Unit = {
    if (weightType == WeightTypes.chiSquare) {
      val numberOfProfileBlocks = pb.blocks.size
      val totalNumberOfBlocks: Double = blockIndex.value.size.toDouble
      for (i <- 0 until neighboursNumber) {
        val neighbourID = neighbours(i)
        if (useEntropy) {
          weights.update(neighbourID, calcChiSquare(weights(neighbourID), profileBlocksSizeIndex.value(neighbourID), numberOfProfileBlocks, totalNumberOfBlocks) * entropies(neighbourID))
        }
        else {
          weights.update(neighbourID, calcChiSquare(weights(neighbourID), profileBlocksSizeIndex.value(neighbourID), numberOfProfileBlocks, totalNumberOfBlocks))
        }
      }
    }
    else if (weightType == WeightTypes.ARCS) {
      val profileID = pb.profileID
      val profileBlocks = pb.blocks
      profileBlocks.foreach { block =>
        val blockID = block.blockID
        val blockProfiles = blockIndex.value.get(blockID)
        if (blockProfiles.isDefined) {
          val comparisons = {
            if (separatorID.isEmpty) {
              val s = blockProfiles.get.head.size.toDouble
              s*(s-1)
            }
            else {
              blockProfiles.get.map(_.size.toDouble).product
            }
          }
          for (i <- 0 until neighboursNumber) {
            val neighbourID = neighbours(i)
            weights.update(neighbourID, weights(neighbourID) / comparisons)
            if (useEntropy) {
              weights.update(neighbourID, weights(neighbourID) * entropies(neighbourID))
            }
          }
        }
      }
    }
    else if (weightType == WeightTypes.JS) {
      val numberOfProfileBlocks = pb.blocks.size
      for (i <- 0 until neighboursNumber) {
        val neighbourID = neighbours(i)
        val commonBlocks = weights(neighbourID)
        val JS = {
          if (useEntropy) {
            (commonBlocks / (numberOfProfileBlocks + profileBlocksSizeIndex.value(neighbourID) - commonBlocks)) * entropies(neighbourID)
          }
          else {
            commonBlocks / (numberOfProfileBlocks + profileBlocksSizeIndex.value(neighbourID) - commonBlocks)
          }
        }
        weights.update(neighbourID, JS)
      }
    }
    else if (weightType == WeightTypes.EJS) {
      val profileNumberOfNeighbours = edgesPerProfile.value.getOrElse(pb.profileID, 0.00000000001)
      val numberOfProfileBlocks = pb.blocks.size
      for (i <- 0 until neighboursNumber) {
        val neighbourID = neighbours(i)
        val commonBlocks = weights(neighbourID)
        val EJS = {
          if (useEntropy) {
            ((commonBlocks / (numberOfProfileBlocks + profileBlocksSizeIndex.value(neighbourID) - commonBlocks)) * entropies(neighbourID)) * Math.log10(numberOfEdges / edgesPerProfile.value.getOrElse(neighbourID, 0.00000000001) * Math.log10(numberOfEdges / profileNumberOfNeighbours))
          }
          else {
            (commonBlocks / (numberOfProfileBlocks + profileBlocksSizeIndex.value(neighbourID) - commonBlocks)) * Math.log10(numberOfEdges / edgesPerProfile.value.getOrElse(neighbourID, 0.00000000001) * Math.log10(numberOfEdges / profileNumberOfNeighbours))
          }
        }
        weights.update(neighbourID, EJS)
      }
    }
    else if (weightType == WeightTypes.ECBS) {
      val blocksNumber = blockIndex.value.size
      val numberOfProfileBlocks = pb.blocks.size
      for (i <- 0 until neighboursNumber) {
        val neighbourID = neighbours(i)
        val commonBlocks = weights(neighbourID)
        val ECBS = {
          if (useEntropy) {
            commonBlocks * Math.log10(blocksNumber / numberOfProfileBlocks) * Math.log10(blocksNumber / profileBlocksSizeIndex.value(neighbourID)) * entropies(neighbourID)
          }
          else {
            commonBlocks * Math.log10(blocksNumber / numberOfProfileBlocks) * Math.log10(blocksNumber / profileBlocksSizeIndex.value(neighbourID))
          }
        }
        weights.update(neighbourID, ECBS)
      }
    }
  }
  def doReset(weights: Array[Double],
              neighbours: Array[Int],
              entropies: Array[Double],
              useEntropy: Boolean,
              neighboursNumber: Int): Unit = {
    for (i <- 0 until neighboursNumber) {
      val neighbourID = neighbours(i)
      weights.update(neighbourID, 0)
      if (useEntropy) {
        entropies.update(neighbourID, 0)
      }
    }
  }
}
