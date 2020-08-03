package org.bd720.ercore.methods.blockrefinement.pruningmethod
import java.util.Calendar
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bd720.ercore.methods.blockrefinement.pruningmethod.PruningUtils.WeightTypes
import org.bd720.ercore.methods.datastructure.{ProfileBlocks, UnweightedEdge}
import scala.collection.mutable.ListBuffer
object WNP {
  def WNP(profileBlocksFiltered: RDD[ProfileBlocks],
          blockIndex: Broadcast[scala.collection.Map[Int, Array[Set[Int]]]],
          maxID: Int,
          separatorIDs: Array[Int],
          groundtruth: Broadcast[scala.collection.immutable.Set[(Int, Int)]],
          thresholdType: String = PruningUtils.ThresholdTypes.AVG,
          weightType: String = WeightTypes.CBS,
          profileBlocksSizeIndex: Broadcast[scala.collection.Map[Int, Int]] = null,
          useEntropy: Boolean = false,
          blocksEntropies: Broadcast[scala.collection.Map[Int, Double]] = null,
          chi2divider: Double = 2.0,
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
    var numberOfEdges: Double = 0
    var edgesPerProfile: Broadcast[scala.collection.Map[Int, Double]] = null
    if (weightType == WeightTypes.EJS) {
      val stats = CommonNodePruning.computeStatistics(profileBlocksFiltered, blockIndex, maxID, separatorIDs)
      numberOfEdges = stats.map(_._1.toDouble).sum()
      edgesPerProfile = sc.broadcast(stats.map(x => (x._2._1, x._2._2.toDouble)).groupByKey().map(x => (x._1, x._2.sum)).collectAsMap())
    }
    val log = LogManager.getRootLogger
    val sTime = Calendar.getInstance()
    val thresholds = sc.broadcast(calcThresholds(profileBlocksFiltered, blockIndex, maxID, separatorIDs, thresholdType, weightType, profileBlocksSizeIndex, useEntropy, blocksEntropies, numberOfEdges, edgesPerProfile).collectAsMap())
    val eTime = Calendar.getInstance()
    log.info("SPARKER - Time to compute thresholds "+((eTime.getTimeInMillis-sTime.getTimeInMillis)/1000.0/60.0) + " min")
    val edges = pruning(profileBlocksFiltered, blockIndex, maxID, separatorIDs, groundtruth, thresholdType, weightType, profileBlocksSizeIndex, useEntropy, blocksEntropies, chi2divider, comparisonType, thresholds, numberOfEdges, edgesPerProfile)
    thresholds.unpersist()
    if (edgesPerProfile != null) {
      edgesPerProfile.unpersist()
    }
    edges
  }
  def calcThreshold(weights: Array[Double],
                    neighbours: Array[Int],
                    neighboursNumber: Int,
                    thresholdType: String): Double = {
    var acc: Double = 0
    for (i <- 0 until neighboursNumber) {
      val neighbourID = neighbours(i)
      if (thresholdType == PruningUtils.ThresholdTypes.AVG) {
        acc += weights(neighbourID)
      }
      else if (thresholdType == PruningUtils.ThresholdTypes.MAX_FRACT_2 && weights(neighbourID) > acc) {
        acc = weights(neighbourID)
      }
    }
    if (thresholdType == PruningUtils.ThresholdTypes.AVG) {
      acc /= neighboursNumber
    }
    else if (thresholdType == PruningUtils.ThresholdTypes.MAX_FRACT_2) {
      acc /= 2.0
    }
    acc
  }
  def doPruning(profileID: Int,
                weights: Array[Double],
                neighbours: Array[Int],
                neighboursNumber: Int,
                groundtruth: Broadcast[scala.collection.immutable.Set[(Int, Int)]],
                weightType: String,
                comparisonType: String,
                thresholds: Broadcast[scala.collection.Map[Int, Double]],
                chi2divider: Double
               ): (Double, Double, Iterable[UnweightedEdge]) = {
    var cont: Double = 0
    var gtFound: Double = 0
    val edges: ListBuffer[UnweightedEdge] = ListBuffer[UnweightedEdge]()
    val profileThreshold = thresholds.value(profileID)
    if (weightType == WeightTypes.chiSquare) {
      for (i <- 0 until neighboursNumber) {
        val neighbourID = neighbours(i)
        val neighbourThreshold = thresholds.value(neighbourID)
        val neighbourWeight = weights(neighbourID)
        val threshold = Math.sqrt(Math.pow(neighbourThreshold, 2) + Math.pow(profileThreshold, 2)) / chi2divider
        if (neighbourWeight >= threshold) {
          cont += 1
          if (groundtruth != null && groundtruth.value.contains((profileID, neighbourID))) {
            gtFound += 1
          }
          edges.append(UnweightedEdge(profileID, neighbours(i)))
        }
      }
    }
    else {
      for (i <- 0 until neighboursNumber) {
        val neighbourID = neighbours(i)
        val neighbourThreshold = thresholds.value(neighbourID)
        val neighbourWeight = weights(neighbourID)
        if (
          (comparisonType == PruningUtils.ComparisonTypes.AND && neighbourWeight >= neighbourThreshold && neighbourWeight >= profileThreshold)
            || (comparisonType == PruningUtils.ComparisonTypes.OR && (neighbourWeight >= neighbourThreshold || neighbourWeight >= profileThreshold))
        ) {
          cont += 1
          if (groundtruth != null && groundtruth.value.contains((profileID, neighbourID))) {
            gtFound += 1
          }
          edges.append(UnweightedEdge(profileID, neighbours(i)))
        } /*
        else{
          log.info("SPARKER - Id mio "+profileID+", ID vicino "+neighbourID+", soglia vicino "+neighbourThreshold+", peso vicino "+neighbourWeight+", soglia mia "+profileThreshold+", comparison type "+comparisonType+" ----> Non lo tengo")
        }*/
      }
    }
    (cont, gtFound, edges)
  }
  def calcThresholds(profileBlocksFiltered: RDD[ProfileBlocks],
                     blockIndex: Broadcast[scala.collection.Map[Int, Array[Set[Int]]]],
                     maxID: Int,
                     separatorID: Array[Int],
                     thresholdType: String,
                     weightType: String,
                     profileBlocksSizeIndex: Broadcast[scala.collection.Map[Int, Int]],
                     useEntropy: Boolean,
                     blocksEntropies: Broadcast[scala.collection.Map[Int, Double]],
                     numberOfEdges: Double,
                     edgesPerProfile: Broadcast[scala.collection.Map[Int, Double]]
                    ): RDD[(Int, Double)] = {
    profileBlocksFiltered.mapPartitions { partition =>
      val localWeights = Array.fill[Double](maxID + 1) {
        0
      }
      val neighbours = Array.ofDim[Int](maxID + 1)
      var neighboursNumber = 0
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
        val threshold = calcThreshold(localWeights, neighbours, neighboursNumber, thresholdType)
        CommonNodePruning.doReset(localWeights, neighbours, eopies, useEntropy, neighboursNumber)
        (pb.profileID, threshold)
      }
    }
  }
  def pruning(profileBlocksFiltered: RDD[ProfileBlocks],
              blockIndex: Broadcast[scala.collection.Map[Int, Array[Set[Int]]]],
              maxID: Int,
              separatorID: Array[Int],
              groundtruth: Broadcast[scala.collection.immutable.Set[(Int, Int)]],
              thresholdType: String,
              weightType: String,
              profileBlocksSizeIndex: Broadcast[scala.collection.Map[Int, Int]],
              useEntropy: Boolean,
              blocksEntropies: Broadcast[scala.collection.Map[Int, Double]],
              chi2divider: Double,
              comparisonType: String,
              thresholds: Broadcast[scala.collection.Map[Int, Double]],
              numberOfEdges: Double,
              edgesPerProfile: Broadcast[scala.collection.Map[Int, Double]]
             ): RDD[(Double, Double, Iterable[UnweightedEdge])] = {
    profileBlocksFiltered.mapPartitions { partition =>
      val localWeights = Array.fill[Double](maxID + 1) {
        0
      }
      val neighbours = Array.ofDim[Int](maxID + 1)
      var neighboursNumber = 0
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
        neighboursNumber = CommonNodePruning.calcCBS(pb, blockIndex, separatorID, useEntropy, blocksEntropies, localWeights, entropies, neighbours, false)
        CommonNodePruning.calcWeights(pb, localWeights, neighbours, entropies, neighboursNumber, blockIndex, separatorID, weightType, profileBlocksSizeIndex, useEntropy, numberOfEdges, edgesPerProfile)
        val result = doPruning(pb.profileID, localWeights, neighbours, neighboursNumber, groundtruth, weightType, comparisonType, thresholds, chi2divider)
        CommonNodePruning.doReset(localWeights, neighbours, entropies, useEntropy, neighboursNumber)
        result
      }
    }
  }
}
