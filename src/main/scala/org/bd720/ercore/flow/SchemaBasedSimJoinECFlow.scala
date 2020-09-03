package org.bd720.ercore.flow
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.bd720.ercore.common.SparkEnvSetup
import org.bd720.ercore.dataloader.filter.SpecificFieldValueFilter
import org.bd720.ercore.dataloader.{DataType, DataTypeResolver, JSONWrapper, ProfileLoaderFactory, ProfileLoaderTrait}
import org.bd720.ercore.datawriter.GenericDataWriter
import org.bd720.ercore.datawriter.GenericDataWriter.generateOutputWithSchema
import org.bd720.ercore.entity.parameter.DataSetConfig
import org.bd720.ercore.methods.datastructure.{KeyValue, Profile, WeightedEdge}
import org.bd720.ercore.methods.entityclustering.ConnectedComponentsClustering
import org.bd720.ercore.methods.similarityjoins.common.CommonFunctions
import org.bd720.ercore.methods.similarityjoins.simjoin.{EDJoin, PartEnum}
import org.bd720.ercore.methods.util.PrintContext.printSparkContext
import org.bd720.ercore.methods.util.{CommandLineUtil, PrintContext}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.io.File
object SchemaBasedSimJoinECFlow extends ERFlow with SparkEnvSetup {
  private val ALGORITHM_EDJOIN = "EDJoin"
  private val ALGORITHM_PARTENUM = "PartEnum"
  override def run(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    printSparkContext()
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
    val showSimilarity = CommandLineUtil.getParameter(args, "showSimilarity", "false")
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
    def includeRealID(c: DataSetConfig): Boolean = c.dataSetId != null && !c.dataSetId.trim.isEmpty && c.attributes.contains(c.dataSetId)
    val keepReadID1 = includeRealID(dataSet1)
    log.info("keepReadID1=" + keepReadID1)
    val profiles1: RDD[Profile] = loadDataWithOption(args, "dataSet1", dataSet1, keepReadID1, 0, 0)
    val keepReadID2 = includeRealID(dataSet2)
    log.info("keepReadID2=" + keepReadID2)
    val numberOfProfile1 = profiles1.count()
    val secondEPStartID = numberOfProfile1.intValue()
    log.info("profiles1 count=" + numberOfProfile1)
    val profiles2: RDD[Profile] = loadDataWithOption(args, "dataSet2", dataSet2, keepReadID2, secondEPStartID, 1)
    log.info("profiles2 count=" + profiles2.count())
    preCheckOnProfile(profiles1)
    preCheckOnProfile(profiles2)
    log.info("profiles1 first=" + profiles1.first())
    log.info("profiles2 first=" + profiles2.first())
    preCheckOnAttributePair(dataSet1, dataSet2)
    val t1 = Calendar.getInstance().getTimeInMillis
    val attributePairsArray = collectAttributesFromProfiles(profiles1, profiles2, dataSet1, dataSet2)
    val flowOptions = FlowOptions.getOptions(args)
    log.info("flowOptions=" + flowOptions)
    val matchDetails = doJoin(flowOptions, attributePairsArray, weighted, weightValues)
    val t2 = Calendar.getInstance().getTimeInMillis
    log.info("[SSJoin] Global join+verification time (s) " + (t2 - t1) / 1000.0)
    log.info("[SSJoin] match attribute pairs " + attributePairsArray.length)
    val nm = matchDetails.count()
    log.info("[SSJoin] Number of matches " + nm)
    val t3 = Calendar.getInstance().getTimeInMillis
    attributePairsArray.foreach(at => {
      Option(at._1).map(_.unpersist())
      Option(at._2).map(_.unpersist())
    })
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
      for (i <- 0 until idArray.length) {
        val target: Int = idArray(i)
        for (j <- i + 1 until idArray.length) {
          val source = idArray(j)
          pairs += ((target, source))
        }
      }
      pairs
    })
    if (!matchedPairs.isEmpty()) {
      matchDetails.take(3).foreach(x => log.info("matchDetails=" + x))
      matchedPairs.take(3).foreach(x => log.info("matchedPair=" + x))
    }
    log.info("matchedPairsCount=" + matchedPairs.count() + ",matchDetails=" + matchDetails.count())
    val showSim = showSimilarity.toBoolean
    val (columnNames, rows) = renderResult(dataSet1Id, moreAttr1s, moreAttr2s,
      dataSet2Id, dataSet1, dataSet2,
      keepReadID1, keepReadID2, secondEPStartID,
      matchDetails, profiles, matchedPairs,
      showSim)
    val overwrite = overwriteOnExist == "true" || overwriteOnExist == "1"
    val finalPath = generateOutputWithSchema(columnNames, rows, outputPath, outputType, joinResultFile, overwrite)
    log.info("save mapping into path " + finalPath)
    log.info("[SSJoin] Completed")
  }
  def renderResult(dataSet1Id: String, moreAttr1s: Array[String], moreAttr2s: Array[String],
                   dataSet2Id: String, dataSet1: DataSetConfig, dataSet2: DataSetConfig,
                   keepReadID1: Boolean, keepReadID2: Boolean, secondEPStartID: Int,
                   matchDetails: RDD[(Int, Int, Double)], profiles: RDD[Profile], matchedPairs: RDD[(Int, Int)],
                   showSimilarity: Boolean): (Seq[String], RDD[Row]) = {
    log.info("showSimilarity=" + showSimilarity)
    val spark = SparkSession.builder().getOrCreate()
    if (showSimilarity) {
      val matchedPairsWithSimilarity = enrichPairs(matchedPairs)
      log.info("matchedPairsWithSimilarityCount=" + matchedPairsWithSimilarity.count())
      val profileMatches2 = mapMatchesWithProfilesAndSimilarity(matchedPairsWithSimilarity, profiles, secondEPStartID)
      log.info("profileMatchesCount=" + profileMatches2.count())
      val matchesInDiffDataSet2 = profileMatches2.filter(t => t._1.sourceId != t._2.sourceId).zipWithIndex()
      log.info("matchesInDiffDataSet2 size =" + matchesInDiffDataSet2.count())
      val finalMap2 = matchesInDiffDataSet2.map(x => (x._1._1.originalID, x._1._2.originalID, x._1._3))
      log.info("finalmap2 size =" + finalMap2.count())
      val p1IDFilterOption = finalMap2.map(x => KeyValue(dataSet1Id, x._1)).toLocalIterator.toList
      val finalProfiles1 = getProfileLoader(dataSet1.path).load(dataSet1.path, realIDField = dataSet1.dataSetId,
        startIDFrom = 0, sourceId = 0, keepRealID = keepReadID1, fieldsToKeep = moreAttr1s.toList,
        fieldValuesScope = p1IDFilterOption,
        filter = SpecificFieldValueFilter
      )
      val p1B = spark.sparkContext.broadcast(finalProfiles1.collect())
      val p2IDFilterOption = finalMap2.map(x => KeyValue(dataSet2Id, x._2)).toLocalIterator.toList
      val finalProfiles2 = getProfileLoader(dataSet2.path).load(dataSet2.path, realIDField = dataSet2.dataSetId,
        startIDFrom = 0, sourceId = 0, keepRealID = keepReadID2, fieldsToKeep = moreAttr2s.toList,
        fieldValuesScope = p2IDFilterOption,
        filter = SpecificFieldValueFilter
      )
      if (!finalProfiles1.isEmpty()) {
        finalProfiles1.take(3).foreach(x => log.info("fp1=" + x))
      }
      if (!finalProfiles2.isEmpty()) {
        finalProfiles2.take(3).foreach(x => log.info("fp2=" + x))
      }
      log.info("fp1count=" + finalProfiles1.count())
      log.info("fp2count=" + finalProfiles2.count())
      val p2B = spark.sparkContext.broadcast(finalProfiles2.collect())
      val columnNames: Seq[String] = resolveColumns(moreAttr1s, moreAttr2s, showSimilarity)
      val rows = finalMap2.map(x => {
        var entry = Seq[String]()
        entry :+= x._3.toString
        entry :+= x._1
        val attr1 = p1B.value.filter(p => p.originalID == x._1).flatMap(p => p.attributes)
        entry ++= moreAttr1s.map(ma => attr1.find(_.key == ma).getOrElse(KeyValue("", "N/A")).value).toSeq
        entry :+= x._2
        val attr2 = p2B.value.filter(p => p.originalID == x._2).flatMap(p => p.attributes)
        entry ++= moreAttr2s.map(ma => attr2.find(_.key == ma).getOrElse(KeyValue("", "N/A")).value).toSeq
        Row.fromSeq(entry)
      })
      (columnNames, rows)
    } else {
      val profileMatches = mapMatchesWithProfiles(matchedPairs, profiles, secondEPStartID)
      if (!profileMatches.isEmpty()) {
        profileMatches.take(3).foreach(x => log.info("profileMatches=" + x))
      }
      log.info("profileMatchesCount=" + profileMatches.count())
      val matchesInDiffDataSet = profileMatches.filter(t => t._1.sourceId != t._2.sourceId).zipWithIndex()
      log.info("[SSJoin] Get matched pairs " + matchesInDiffDataSet.count())
      if (!matchesInDiffDataSet.isEmpty()) {
        matchesInDiffDataSet.take(3).foreach(t => {
          log.info("matches-pair=" +
            (t._2, (t._1._1.originalID, t._1._1.sourceId), (t._1._2.originalID, t._1._2.sourceId)))
        })
      }
      log.info("moreAttr1s=" + moreAttr1s.toList)
      log.info("moreAttr2s=" + moreAttr2s.toList)
      log.info("matchesInDiffDataSet1 size =" + matchesInDiffDataSet.count())
      val finalMap = matchesInDiffDataSet.map(x => (x._1._1.originalID, x._1._2.originalID))
      log.info("finalmap1 size =" + finalMap.count())
      val p1IDFilterOption = finalMap.map(x => KeyValue(dataSet1Id, x._1)).toLocalIterator.toList
      val finalProfiles1 = getProfileLoader(dataSet1.path).load(dataSet1.path, realIDField = dataSet1.dataSetId,
        startIDFrom = 0, sourceId = 0, keepRealID = keepReadID1, fieldsToKeep = moreAttr1s.toList,
        fieldValuesScope = p1IDFilterOption,
        filter = SpecificFieldValueFilter
      )
      val p1B = spark.sparkContext.broadcast(finalProfiles1.collect())
      val p2IDFilterOption = finalMap.map(x => KeyValue(dataSet2Id, x._2)).toLocalIterator.toList
      val finalProfiles2 = getProfileLoader(dataSet2.path).load(dataSet2.path, realIDField = dataSet2.dataSetId,
        startIDFrom = 0, sourceId = 0, keepRealID = keepReadID2, fieldsToKeep = moreAttr2s.toList,
        fieldValuesScope = p2IDFilterOption,
        filter = SpecificFieldValueFilter
      )
      if (!finalProfiles1.isEmpty()) {
        finalProfiles1.take(3).foreach(x => log.info("fp1=" + x))
      }
      if (!finalProfiles2.isEmpty()) {
        finalProfiles2.take(3).foreach(x => log.info("fp2=" + x))
      }
      log.info("fp1count=" + finalProfiles1.count())
      log.info("fp2count=" + finalProfiles2.count())
      val p2B = spark.sparkContext.broadcast(finalProfiles2.collect())
      val columnNames: Seq[String] = resolveColumns(moreAttr1s, moreAttr2s, showSimilarity)
      val rows = finalMap.map(x => {
        var entry = Seq[String]()
        entry :+= x._1
        val attr1 = p1B.value.filter(p => p.originalID == x._1).flatMap(p => p.attributes)
        entry ++= moreAttr1s.map(ma => attr1.find(_.key == ma).getOrElse(KeyValue("", "N/A")).value).toSeq
        entry :+= x._2
        val attr2 = p2B.value.filter(p => p.originalID == x._2).flatMap(p => p.attributes)
        entry ++= moreAttr2s.map(ma => attr2.find(_.key == ma).getOrElse(KeyValue("", "N/A")).value).toSeq
        Row.fromSeq(entry)
      })
      (columnNames, rows)
    }
  }
  private def doJoin(flowOptions: Map[String, String], attributePairsArray: ArrayBuffer[(RDD[(Int, String)], RDD[(Int, String)])],
                     weighted: Boolean, weights: List[Double]) = {
    val q = flowOptions.get("q").getOrElse("2")
    val algorithm = flowOptions.get("algorithm").getOrElse(ALGORITHM_EDJOIN)
    val threshold = flowOptions.get("threshold").getOrElse("2")
    def getMatches(pair: (RDD[(Int, String)], RDD[(Int, String)])): RDD[(Int, Int, Double)] = {
      algorithm match {
        case ALGORITHM_EDJOIN =>
          val attributes = pair._1.union(pair._2)
          EDJoin.getMatches(attributes, q.toInt, threshold.toInt)
        case ALGORITHM_PARTENUM =>
          val attributes = pair._1.union(pair._2)
          PartEnum.getMatches(attributes, threshold.toDouble)
      }
    }
    val attributesMatches: RDD[(Int, Int, Double)] = getMatches(attributePairsArray(0))
    for (i <- 1 until attributePairsArray.length) {
      val next = attributePairsArray(i)
      if (weighted) {
        val nextMatches = attributesMatches.union(getMatches(next))
        nextMatches.groupBy(x => (x._1, x._2)).map(x => x._2.reduce((y, z) => (y._1, y._2, y._3 + z._3)))
      } else {
        attributesMatches.intersection(getMatches(next))
      }
    }
    attributesMatches
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
  private def enrichPairs(matchPairs: RDD[(Int, Int)]): RDD[(Int, Int, Double)] = {
    matchPairs.map(x => (x._1, x._2, 1.0))
  }
  private def enrichWithSimilarity(matchPairs: RDD[(Int, Int)], matchDetails: RDD[(Int, Int, Double)], secondEPStartID: Int): RDD[(Int, Int, Double)] = {
    val mp = matchPairs.keyBy(x => (x._1, x._2))
    val detail = matchDetails.keyBy(x => (x._1, x._2))
    val data = mp.join(detail)
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
  private def loadDataWithOption(args: Array[String], dataSetPrefix: String, dataSetConfig: DataSetConfig,
                                 keepRealID: Boolean, epStartID: Int, sourceId: Int): RDD[Profile] = {
    val options = FilterOptions.getOptions(dataSetPrefix, args)
    log.info(dataSetPrefix + "-FilterOptions=" + options)
    val path = dataSetConfig.path
    val loader = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(path))
    log.info("profileLoader is " + loader)
    val data = loader.load(
      path, realIDField = dataSetConfig.dataSetId,
      startIDFrom = epStartID,
      sourceId = sourceId, keepRealID = keepRealID,
      fieldsToKeep = dataSetConfig.attributes.toList,
      fieldValuesScope = options,
      filter = SpecificFieldValueFilter)
    data
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
  private def weightedMatches(attributesMatches: Array[RDD[(Int, Int, Double)]], weights: List[Double]): RDD[(Int, Int, Double)] = {
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
  private def preCheckOnProfile(profiles: RDD[Profile]) = {
    if (profiles.isEmpty()) {
      throw new RuntimeException("Empty profile data set")
    }
  }
  def collectAttributesFromProfiles(profiles1: RDD[Profile], profiles2: RDD[Profile], dataSet1: DataSetConfig, dataSet2: DataSetConfig): ArrayBuffer[(RDD[(Int, String)], RDD[(Int, String)])] = {
    var attributesArray = new ArrayBuffer[(RDD[(Int, String)], RDD[(Int, String)])]()
    log.info("dataSet1Attr=" + dataSet1.attributes.toList + " vs dataSet2Attr=" + dataSet2.attributes.toList)
    for (i <- 0 until dataSet1.attributes.length) {
      val attributes1 = CommonFunctions.extractField(profiles1, dataSet1.attributes(i))
      val attributes2 = Option(dataSet2.attributes).map(x => CommonFunctions.extractField(profiles2, x(i))).orNull
      attributesArray :+= ((attributes1, attributes2))
    }
    log.info("attrsArrayLength=" + attributesArray.length)
    if (attributesArray.length > 0) {
      log.info("attrsArrayHead _1count=" + attributesArray.head._1.count() + ", _2count=" + attributesArray.head._2.count())
      log.info("attrsArrayHead _1first=" + attributesArray.head._1.first() + ", _2first=" + attributesArray.head._2.first())
    }
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
