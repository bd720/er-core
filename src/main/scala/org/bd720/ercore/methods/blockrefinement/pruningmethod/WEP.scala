package org.bd720.ercore.methods.blockrefinement.pruningmethod
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bd720.ercore.methods.blockrefinement.pruningmethod.PruningUtils.WeightTypes
import org.bd720.ercore.methods.datastructure.{ProfileBlocks, UnweightedEdge}
object WEP {
  def WEP(profileBlocksFiltered: RDD[ProfileBlocks],
          blockIndex: Broadcast[scala.collection.Map[Int, Array[Set[Int]]]],
          maxID: Int,
          separatorIDs: Array[Int],
          groundtruth: Broadcast[scala.collection.immutable.Set[(Int, Int)]],
          weightType: String = WeightTypes.CBS,
          profileBlocksSizeIndex: Broadcast[scala.collection.Map[Int, Int]] = null,
          useEntropy: Boolean = false,
          blocksEntropies: Broadcast[scala.collection.Map[Int, Double]] = null
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
    val sc = SparkContext.getOrCreate()
    var numberOfEdges: Double = 0
    var edgesPerProfile: Broadcast[scala.collection.Map[Int, Double]] = null
    if (weightType == WeightTypes.EJS) {
      val stats = CommonNodePruning.computeStatistics(profileBlocksFiltered, blockIndex, maxID, separatorIDs)
      numberOfEdges = stats.map(_._1.toDouble).sum()
      edgesPerProfile = sc.broadcast(stats.map(x => (x._2._1, x._2._2.toDouble)).groupByKey().map(x => (x._1, x._2.sum)).collectAsMap())
    }
    val threshold = calcThreshold(profileBlocksFiltered, blockIndex, maxID, separatorIDs, weightType, profileBlocksSizeIndex, useEntropy, blocksEntropies, numberOfEdges, edgesPerProfile)
    val edges = pruning(profileBlocksFiltered, blockIndex, maxID, separatorIDs, groundtruth, weightType, profileBlocksSizeIndex, useEntropy, blocksEntropies, threshold, numberOfEdges, edgesPerProfile)
    if (edgesPerProfile != null) {
      edgesPerProfile.unpersist()
    }
    edges
  }
  def calcSum(weights: Array[Double],
              neighbours: Array[Int],
              neighboursNumber: Int): Double = {
    var acc: Double = 0
    for (i <- 0 until neighboursNumber) {
      val neighbourID = neighbours(i)
      acc += weights(neighbourID)
    }
    acc
  }
  def doPruning(profileID: Int,
                weights: Array[Double],
                neighbours: Array[Int],
                neighboursNumber: Int,
                groundtruth: Broadcast[scala.collection.immutable.Set[(Int, Int)]],
                weightType: String,
                globalThreshold: Double
               ): (Double, Double, Iterable[UnweightedEdge]) = {
    var cont: Double = 0
    var gtFound: Double = 0
    var edges: List[UnweightedEdge] = Nil
    for (i <- 0 until neighboursNumber) {
      val neighbourID = neighbours(i)
      val neighbourWeight = weights(neighbourID)
      if (neighbourWeight >= globalThreshold) {
        cont += 1
        if (groundtruth != null && groundtruth.value.contains((profileID, neighbourID))) {
          gtFound += 1
        }
        edges = UnweightedEdge(profileID, neighbours(i)) :: edges
      } /*
        else{
          log.info("SPARKER - Id mio "+profileID+", ID vicino "+neighbourID+", soglia vicino "+neighbourThreshold+", peso vicino "+neighbourWeight+", soglia mia "+profileThreshold+", comparison type "+comparisonType+" ----> Non lo tengo")
        }*/
    }
    (cont, gtFound, edges)
  }
  def calcThreshold(profileBlocksFiltered: RDD[ProfileBlocks],
                    blockIndex: Broast[scala.collection.Map[Int, Array[Set[Int]]]],
                    maxID: Int,
                    separatorID: Array[Int],
                    weightType: String,
                    profileBlocksSizeIndex: Broadcast[scala.collection.Map[Int, Int]],
                    useEntropy: Boolean,
                    blocksEntropies: Broadcast[scala.collection.Map[Int, Double]],
                    numberOfEdges: Double,
                    edgesPerProfile: Broadcast[scala.collection.Map[Int, Double]]
                   ): Double = {
    val partialSums = profileBlocksFiltered.mapPartitions { partition =>
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
        val localSum = calcSum(localWeights, neighbours, neighboursNumber)
        CommonNodePruning.doReset(localWeights, neighbours, entropies, useEntropy, neighboursNumber)
        (localSum, neighboursNumber.toDouble)
      }
    }
    val sums = partialSums.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    sums._1 / sums._2
  }
  def pruning(profileBlocksFiltered: RDD[ProfileBlocks],
              blockIndex: Broadcast[scala.collection.Map[Int, Array[Set[Int]]]],
              maxID: Int,
              separatorID: Array[Int],
              groundtruth: Broadcast[scala.collection.immutable.Set[(Int, Int)]],
              weightType: String,
              profileBlocksSizeIndex: Broadcast[scala.collection.Map[Int, Int]],
              useEntropy: Boolean,
              blocksEntropies: Broadcast[scala.collection.Map[Int, Double]],
              threshold: Double,
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
        val result = doPruning(pb.profileID, localWeights, neighbours, neighboursNumber, groundtruth, weightType, threshold)
        CommonNodePruning.doReset(localWeights, neighbours, entropies, useEntropy, neighboursNumber)
        result
      }
    }
  }
}
