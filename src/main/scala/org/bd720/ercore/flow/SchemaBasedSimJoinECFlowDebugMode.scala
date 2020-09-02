package org.bd720.ercore.flow
import java.util.Calendar
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.bd720.ercore.common.SparkEnvSetup
import org.bd720.ercore.dataloader.filter.SpecificFieldValueFilter
import org.bd720.ercore.dataloader.{DataTypeResolver, ProfileLoaderFactory, ProfileLoaderTrait}
import org.bd720.ercore.datawriter.GenericDataWriter.generateOutputWithSchema
import org.bd720.ercore.entity.parameter.DataSetConfig
import org.bd720.ercore.methods.datastructure.{KeyValue, Profile, WeightedEdge}
import org.bd720.ercore.methods.entityclustering.ConnectedComponentsClustering
import org.bd720.ercore.methods.similarityjoins.common.CommonFunctions
import org.bd720.ercore.methods.similarityjoins.simjoin.{EDJoin, PartEnum}
import org.bd720.ercore.methods.util.CommandLineUtil
import scala.collection.mutable.ArrayBuffer
object SchemaBasedSimJoinECFlowDebugMode extends ERFlow with SparkEnvSetup {
  private val ALGORITHM_EDJOIN = "EDJoin"
  private val ALGORITHM_PARTENUM = "PartEnum"
  override def run(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    printContext(spark)
    val flowOptions = FlowOptions.getOptions(args)
    log.info("flowOptions=" + flowOptions)
    val dataSet1Path = CommandLineUtil.getParameter(args, "dataSet1", "datasets/clean/DblpAcm/dataset1.json")
    val dataSet1Format = CommandLineUtil.getParameter(args, "dataSet1-format", "json")
    val dataSet1Id = CommandLineUtil.getParameter(args, "dataSet1-id", "realProfileID")
    val attributes1 = CommandLineUtil.getParameter(args, "dataSet1-attrSet", "title")
    val moreAttr1ToExtract = CommandLineUtil.getParameter(args, "dataSet1-additionalAttrSet", "title")
    val moreAttr1s = moreAttr1ToExtract.split(",")
    val moreAttr2ToExtract = CommandLineUtil.getParameter(args, "dataSet2-additionalAttrSet", "title")
    val moreAttr2s = moreAttr2ToExtract.split(",")
    val dataSet2Path = CommandLineUtil.getParameter(args, "dataSet2", "datasets/clean/DblpAcm/dataset2.json")
    val dataSet2Format = CommandLineUtil.getParameter(args, "dataSet2-format", "json")
    val dataSet2Id = CommandLineUtil.getParameter(args, "dataSet2-id", "realProfileID")
    val attributes2 = CommandLineUtil.getParameter(args, "dataSet2-attrSet", "title")
    val outputPath = CommandLineUtil.getParameter(args, "outputPath", "output/mapping")
    val outputType = CommandLineUtil.getParameter(args, "outputType", "json")
    val joinResultFile = CommandLineUtil.getParameter(args, "joinResultFile", "mapping")
    val overwriteOnExist = CommandLineUtil.getParameter(args, "overwriteOnExist", "false")
    val joinFieldsWeight = CommandLineUtil.getParameter(args, "joinFieldsWeight", "")
    val dataSet1 = new DataSetConfig(dataSet1Path, dataSet1Format, dataSet1Id,
      Option(attributes1).map(_.split(",")).orNull)
    val dataSet2 = new DataSetConfig(dataSet2Path, dataSet2Format, dataSet2Id,
      Option(attributes2).map(_.split(",")).orNull)
    log.info("dataSet1=" + dataSet1)
    log.info("dataSet2=" + dataSet2)
    preCheckOnAttributePair(dataSet1, dataSet2)
    val weighted = joinFieldsWeight != null && joinFieldsWeight.trim != ""
    val weightValues = checkAndResolveWeights(joinFieldsWeight, dataSet1)
    preCheckOnWeight(weightValues)
    def profileLoader: ProfileLoaderTrait = getProfileLoader(dataSet1.path)
    log.debug("resolve profile loader " + profileLoader)
    def includeRealID(c: DataSetConfig): Boolean = c.dataSetId != null && !c.dataSetId.trim.isEmpty && c.attributes.contains(c.dataSetId)
    val keepReadID1 = includeRealID(dataSet1)
    log.info("keepReadID1=" + keepReadID1)
    val p1FilterOptions = FilterOptions.getOptions("dataSet1", args)
    log.info("p1FilterOptions=" + p1FilterOptions)
    val profiles1 = profileLoader.load(dataSet1.path, realIDField = dataSet1.dataSetId,
      startIDFrom = 0, sourceId = 0, keepRealID = keepReadID1,
      fieldsToKeep = dataSet1.attributes.toList,
      fieldValuesScope = p1FilterOptions,
      filter = SpecificFieldValueFilter
    )
    val keepReadID2 = includeRealID(dataSet2)
    log.info("keepReadID2=" + keepReadID2)
    val secondEPStartID = profiles1.count().intValue()
    log.info("profiles1 count=" + profiles1.count())
    val profiles2: RDD[Profile] = loadDataWithOption(args, dataSet2,
      profileLoader _,
      keepReadID2, secondEPStartID)
    log.info("profiles2 count=" + profiles2.count())
    preCheckOnProfile(profiles1)
    preCheckOnProfile(profiles2)
    log.info("profiles1 first=" + profiles1.first())
    log.info("profiles2 first=" + profiles2.first())
    assert(dataSet2.path == null || dataSet1.attributes.length == dataSet2.attributes.length,
      "If dataSet 2 exist, the number of attribute use to compare between dataSet 1 and dataSet 2 should be equal")
    val attributePairsArray = collectAttributesFromProfiles(profiles1, profiles2, dataSet1, dataSet2)
    val q = flowOptions.get("q").getOrElse("2")
    val algorithm = flowOptions.get("algorithm").getOrElse(ALGORITHM_EDJOIN)
    val threshold = flowOptions.get("threshold").getOrElse("2")
    val t1 = Calendar.getInstance().getTimeInMillis
    var attributesMatches = new ArrayBuffer[RDD[(Int, Int, Double)]]()
    var attributeses = ArrayBuffer[RDD[(Int, String)]]()
    attributePairsArray.foreach(attributesTuple => {
      val attributes1 = attributesTuple._1
      val attributes2 = attributesTuple._2
      log.info("attributes1 first = " + attributes1.first() + " 2 " + attributes2.first())
      val attributesMatch: RDD[(Int, Int, Double)] =
        algorithm match {
          case ALGORITHM_EDJOIN =>
            val attributes = attributes1.union(attributes2)
            attributeses += attributes
            attributes.cache()
            EDJoin.getMatches(attributes, q.toInt, threshold.toInt)
          case ALGORITHM_PARTENUM =>
            val attributes = attributes1.union(attributes2)
            attributeses += attributes
            attributes.cache()
            PartEnum.getMatches(attributes, threshold.toDouble)
        }
      attributesMatches += attributesMatch
    })
    val t2 = Calendar.getInstance().getTimeInMillis
    log.info("[SSJoin] Global join+verification time (s) " + (t2 - t1) / 1000.0)
    log.info("[SSJoin] match attribute pairs " + attributesMatches.length)
    val matchDetails: RDD[(Int, Int, Double)] = if (!weighted) {
      log.info("run with weightedMatches")
      weightedMatches(spark, attributesMatches.toArray, weightValues)
    } else {
      log.info("run with intersectionMatches")
      intersectionMatches(attributesMatches.toArray)
    }
    val nm = matchDetails.count()
    log.info("[SSJoin] Number of matches " + nm)
    val t3 = Calendar.getInstance().getTimeInMillis
    attributePairsArray.foreach(attributesTuple => {
      Option(attributesTuple._1).map(_.unpersist())
      Option(attributesTuple._2).map(_.unpersist())
    })
    attributeses.foreach(_.unpersist())
    log.info("[SSJoin] Intersection time (s) " + (t3 - t2) / 1000.0)
    if (nm > 0) {
      log.info("[SSJoin] First matches " + matchDetails.first())
    }
    val profiles = profiles1.union(profiles2)
    val clusters = ConnectedComponentsClustering.getClusters(profiles,
      matchDetails.map(x => WeightedEdge(x._1, x._2, x._3)), maxProfileID = 0, edgesThreshold = 0.0)
    clusters.cache()
    val cn = clusters.count()
    val t4 = Calendar.getInstance().getTimeInMillis
    log.info("[SSJoin] Number of clusters " + cn)
    log.info("[SSJoin] Clustering time (s) " + (t4 - t3) / 1000.0)
    log.info("[SSJoin] Total time (s) " + (t4 - t1) / 1000.0)
    val matchedPairs = clusters.map(_._2).flatMap(idSet => {
      val pairs = new ArrayBuffer[(Int, Int)]()
      val idArray = idSet.toArray
      log.info("id array " + idSet)
      for (i <- 0 until idArray.length) {
        val target: Int = idArray(i)
        for (j <- i + 1 until idArray.length) {
          val source = idArray(j)
          log.info("add pair " + (target, source))
          pairs += ((target, source))
        }
      }
      pairs
    })
    log.info("matchedPairs=" + matchedPairs.count())
    log.info("matchDetails=" + matchDetails.count())
    val matchedPairsWithSimilarity = enrichWithSimilarity(matchedPairs, matchDetails, secondEPStartID)
    log.info("matchedPairsWithSimilarity=" + matchedPairsWithSimilarity.count())
    val profileMatches = mapMatchesWithProfiles(matchedPairs, profiles, secondEPStartID)
    val profileMatches2 = mapMatchesWithProfilesAndSimilarity(matchedPairsWithSimilarity, profiles, secondEPStartID)
    log.info("profileMatches size =" + profileMatches.count())
    log.info("profileMatches2 size =" + profileMatches2.count())
    val matchesInDiffDataSet2 = profileMatches2.filter(t => t._1.sourceId != t._2.sourceId).zipWithIndex()
    profileMatches.take(5).foreach(x => log.info("profileMatches=" + x))
    val matchesInDiffDataSet = profileMatches.filter(t => t._1.sourceId != t._2.sourceId).zipWithIndex()
    log.info("[SSJoin] Get matched pairs " + matchesInDiffDataSet.count())
    matchesInDiffDataSet.take(5).foreach(t => {
      log.info("matches-pair=" +
        (t._2, (t._1._1.originalID, t._1._1.sourceId), (t._1._2.originalID, t._1._2.sourceId)))
    })
    log.info("moreAttr1s=" + moreAttr1s.toList)
    log.info("moreAttr2s=" + moreAttr2s.toList)
    log.info("matchesInDiffDataSet2 size =" + matchesInDiffDataSet2.count())
    log.info("matchesInDiffDataSet1 size =" + matchesInDiffDataSet.count())
    val finalMap2 = matchesInDiffDataSet2.map(x => (x._1._1.originalID, x._1._2.originalID, x._1._3))
    val finalMap = matchesInDiffDataSet.map(x => (x._1._1.originalID, x._1._2.originalID))
    log.info("finalmap2 size =" + finalMap2.count())
    log.info("finalmap1 size =" + finalMap.count())
    val p1IDFilterOption = finalMap2.map(x => KeyValue(dataSet1Id, x._1)).toLocalIterator.toList
    val finalProfiles1 = profileLoader.load(dataSet1.path, realIDField = dataSet1.dataSetId,
      startIDFrom = 0, sourceId = 0, keepRealID = keepReadID1, fieldsToKeep = moreAttr1s.toList,
      fieldValuesScope = p1IDFilterOption,
      filter = SpecificFieldValueFilter
    )
    val p1B = spark.sparkContext.broadcast(finalProfiles1.collect())
    val p2IDFilterOption = finalMap2.map(x => KeyValue(dataSet2Id, x._2)).toLocalIterator.toList
    val finalProfiles2 = profileLoader.load(dataSet2.path, realIDField = dataSet2.dataSetId,
      startIDFrom = 0, sourceId = 0, keepRealID = keepReadID2, fieldsToKeep = moreAttr2s.toList,
      fieldValuesScope = p2IDFilterOption,
      filter = SpecificFieldValueFilter
    )
    finalProfiles1.foreach(x => log.info("fp1 " + x))
    finalProfiles2.foreach(x => log.info("fp2 " + x))
    log.info("p1b=" + finalProfiles1.count())
    log.info("p2b=" + finalProfiles2.count())
    val p2B = spark.sparkContext.broadcast(finalProfiles2.collect())
    val showSimilarity = true
    val columnNames: Seq[String] = resolveColumns(moreAttr1s, moreAttr2s, showSimilarity)
    val rows = finalMap2.map(x => {
      var entry = Seq[String]()
      if (showSimilarity) {
        entry :+= x._3.toString
      }
      entry :+= x._1
      val attr1 = p1B.value.filter(p => p.originalID == x._1).flatMap(p => p.attributes)
      entry ++= moreAttr1s.map(ma => attr1.find(_.key == ma).getOrElse(KeyValue("", "N/A")).value).toSeq
      entry :+= x._2
      val attr2 = p2B.value.filter(p => p.originalID == x._2).flatMap(p => p.attributes)
      entry ++= moreAttr2s.map(ma => attr2.find(_.key == ma).getOrElse(KeyValue("", "N/A")).value).toSeq
      Row.fromSeq(entry)
    })
    val overwrite = overwriteOnExist == "true" || overwriteOnExist == "1"
    val finalPath = generateOutputWithSchema(columnNames, rows, outputPath, outputType, joinResultFile, overwrite)
    log.info("save mapping into path " + finalPath)
    log.info("[SSJoin] Completed")
  }
  private def checkAndResolveWeights(joinFieldsWeight: String, dataSet1: DataSetConfig) = {
    val weights = joinFieldsWeight.split(',').toList
    if (weights.size != dataSet1.attributes.size) {
      throw new RuntimeException("Cannot resolve same weight size as the given attributes size ")
    }
    weights.map(_.toDouble)
  }
  private def preCheckOnAttributePair(dataSet1: DataSetConfig, dataSet2: DataSetConfig) = {
    if (dataSet1.attributes.size == 0 || dataSet2.attributes.size == 0) {
      throw new RuntimeException("Cannot join data set with no attributed")
    }
    if (dataSet1.attributes.size != dataSet2.attributes.size) {
      throw new RuntimeException("Cannot join if the attribute pair size not same on two data set")
    }
  }
  private def enrichWithSimilarity(matchPairs: RDD[(Int, Int)], matchDetails: RDD[(Int, Int, Double)], secondEPStartID: Int): RDD[(Int, Int, Double)] = {
    val mp = matchPairs.map(x => {
      if (x._1 < secondEPStartID) {
        x
      } else {
        (x._2, x._1)
      }
    }).keyBy(x => (x._1, x._2))
    val detail = matchDetails.map(x => {
      if (x._1 < secondEPStartID) {
        x
      } else {
        (x._2, x._1, x._3)
      }
    }).keyBy(x => (x._1, x._2))
    val data = mp.join(detail)
    log.info("print mp")
    mp.foreach(x => log.info("mp " + x))
    data.foreach(x => log.info("data " + x))
    data.map(x => x._2._2)
  }
  private def resolveColumns(moreAttr1s: Array[String], moreAttr2s: Array[String], showSimilarity: Boolean = false) = {
    var columnNames = Seq[String]()
    if (showSimilarity) {
      columnNames :+= "Similarity"
    }
    val profile1Prefix = "P1-"
    val profile2Prefix = "P2-"
    columnNames :+= profile1Prefix + "ID"
    columnNames ++= moreAttr1s.map(x => profile1Prefix + x)
    columnNames :+= profile2Prefix + "ID"
    columnNames ++= moreAttr2s.map(x => profile2Prefix + x)
    columnNames
  }
  private def loadDataWithOption(args: Array[String], dataSet2: DataSetConfig, profileLoader: () => ProfileLoaderTrait, keepReadID2: Boolean, secondEPStartID: Int) = {
    val p2FilterOptions = FilterOptions.getOptions("dataSet2", args)
    log.info("p2FilterOptions=" + p2FilterOptions)
    val profiles2 = profileLoader().load(
      dataSet2.path, realIDField = dataSet2.dataSetId,
      startIDFrom = secondEPStartID,
      sourceId = 1, keepRealID = keepReadID2,
      fieldsToKeep = dataSet2.attributes.toList,
      fieldValuesScope = p2FilterOptions,
      filter = SpecificFieldValueFilter)
    profiles2
  }
  private def preCheckOnWeight(weights: List[Double]) = {
    val sum = weights.reduce(_ + _)
    if (sum != 1.0) {
      throw new RuntimeException("Cannot continue with weights summary > 1.0, sum=" + sum + " given weights=" + weights)
    }
  }
  private def intersectionMatches(attributesMatches: Array[RDD[(Int, Int, Double)]]): RDD[(Int, Int, Double)] = {
    var matches = attributesMatches(0);
    for (i <- 1 until attributesMatches.length) {
      matches = matches.intersection(attributesMatches(i))
    }
    if (attributesMatches.length > 1) matches.cache()
    matches
  }
  private def weightedMatches(spark: SparkSession, attributesMatches: Array[RDD[(Int, Int, Double)]], weights: List[Double]): RDD[(Int, Int, Double)] = {
    var matches = attributesMatches(0)
    matches = matches.map(x => (x._1, x._2, x._3 * weights(0)))
    for (i <- 1 until attributesMatches.length) {
      var next = attributesMatches(i)
      next = matches.map(x => (x._1, x._2, x._3 * weights(i)))
      matches = matches.union(next)
    }
    if (attributesMatches.length > 1) matches.cache()
    val data = matches.groupBy(x => (x._1, x._2)).map(x => x._2.reduce((y, z) => (y._1, y._2, y._3 + z._3)))
    data
  }
  private def printContext(spark: SparkSession) = {
    log.info("-sparkContext master=" + spark.sparkContext.master)
    log.info("-sparkContext user=" + spark.sparkContext.sparkUser)
    log.info("-sparkContext startTime=" + spark.sparkContext.startTime)
    log.info("-sparkContext appName=" + spark.sparkContext.appName)
    log.info("-sparkContext applicationId=" + spark.sparkContext.applicationId)
    log.info("-sparkContext getConf=" + spark.sparkContext.getConf)
  }
  private def preCheckOnProfile(profiles: RDD[Profile]) = {
    if (profiles.isEmpty()) {
      throw new RuntimeException("Empty profile data set")
    }
  }
  def collectAttributesFromProfiles(profiles1: RDD[Profile], profiles2: RDD[Profile], dataSet1: DataSetConfig, dataSet2: DataSetConfig): ArrayBuffer[(RDD[(Int, String)], RDD[(Int, String)])] = {
    var attributesArray = new ArrayBuffer[(RDD[(Int, String)], RDD[(Int, String)])]()
    log.info("dataSet1Attr=" + dataSet1.attributes.toList)
    log.info("dataSet2Attr=" + dataSet2.attributes.toList)
    for (i <- 0 until dataSet1.attributes.length) {
      val attributes1 = CommonFunctions.extractField(profiles1, dataSet1.attributes(i))
      val attributes2 = Option(dataSet2.attributes).map(attributes => CommonFunctions.extractField(profiles2, attributes(i))).orNull
      attributesArray :+= ((attributes1, attributes2))
    }
    log.info("attributesArray count=" + attributesArray.length)
    log.info("attributesArray _1count=" + attributesArray.head._1.count() + ", _2count=" + attributesArray.head._2.count())
    log.info("attributesArray _1first=" + attributesArray.head._1.first() + ", _2first=" + attributesArray.head._2.first())
    attributesArray
  }
  def mapMatchesWithProfiles(matchedPairs: RDD[(Int, Int)], profiles: RDD[Profile], secondEPStartID: Int): RDD[(Profile, Profile)] = {
    val profilesById = profiles.keyBy(_.id)
    val matchedPairsById = matchedPairs.keyBy(_._1)
    val joinResult = matchedPairsById.join(profilesById)
    joinResult.
      map(t => (t._2._1._2, t._2._2)).keyBy(_._1).
      join(profilesById).map(t => {
      val id1 = t._2._1._2
      val id2 = t._2._2
      if (id1.id < secondEPStartID) {
        (id1, id2)
      } else {
        (id2, id1)
      }
    })
  }
  def mapMatchesWithProfilesAndSimilarity(matchedPairs: RDD[(Int, Int, Double)], profiles: RDD[Profile], secondEPStartID: Int): RDD[(Profile, Profile, Double)] = {
    val profilesById = profiles.keyBy(_.id)
    val matchedPairsById = matchedPairs.keyBy(_._1)
    val joinResult = matchedPairsById.join(profilesById)
    joinResult.
      map(t => (t._2._1._2, t._2._2, t._2._1._3)).keyBy(_._1).
      join(profilesById).map(t => {
      val id1 = t._2._1._2
      val id2 = t._2._2
      val similarity = t._2._1._3
      if (id1.id < secondEPStartID) {
        (id1, id2, similarity)
      } else {
        (id2, id1, similarity)
      }
    })
  }
  def getProfileLoader(dataFile: String): ProfileLoaderTrait = {
    ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(dataFile))
  }
}
