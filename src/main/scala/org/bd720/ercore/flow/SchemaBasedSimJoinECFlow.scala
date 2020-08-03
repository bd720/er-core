package org.bd720.ercore.flow
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.bd720.ercore.common.SparkEnvSetup
import org.bd720.ercore.dataloader.{DataType, DataTypeResolver, JSONWrapper, ProfileLoaderFactory, ProfileLoaderTrait}
import org.bd720.ercore.entity.parameter.DataSetConfig
import org.bd720.ercore.flow.SchemaBasedSimJoinECFlow.log
import org.bd720.ercore.methods.datastructure.{Profile, WeightedEdge}
import org.bd720.ercore.methods.entityclustering.ConnectedComponentsClustering
import org.bd720.ercore.methods.similarityjoins.common.CommonFunctions
import org.bd720.ercore.methods.similarityjoins.simjoin.{EDJoin, PartEnum}
import org.bd720.ercore.methods.util.CommandLineUtil
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.io.File
object SchemaBasedSimJoinECFlow extends ERFlow with SparkEnvSetup {
  private val ALGORITHM_EDJOIN = "EDJoin"
  private val ALGORITHM_PARTENUM = "PartEnum"
  override def run(args: Array[String]): Unit = {
    val outputDir: File = File("/tmp/data-er")
    if (!outputDir.exists) {
      outputDir.createDirectory(true)
    }
    val options = FlowOptions.getOptions(args)
    log.info("flowOptions=" + options)
    val spark = createLocalSparkSession(getClass.getName, outputDir = outputDir.path)
    printContext(spark)
    val dataSet1Path = CommandLineUtil.getParameter(args, "dataSet1", "datasets/clean/DblpAcm/dataset1.json")
    val dataSet1Format = CommandLineUtil.getParameter(args, "dataSet1-format", "json")
    val dataSet1Id = CommandLineUtil.getParameter(args, "dataSet1-id", "realProfileID")
    val attributes1 = CommandLineUtil.getParameter(args, "dataSet1-attrSet", "title")
    val dataSet2Path = CommandLineUtil.getParameter(args, "dataSet2", "datasets/clean/DblpAcm/dataset2.json")
    val dataSet2Format = CommandLineUtil.getParameter(args, "dataSet2-format", "json")
    val dataSet2Id = CommandLineUtil.getParameter(args, "dataSet2-id", "realProfileID")
    val attributes2 = CommandLineUtil.getParameter(args, "dataSet2-attrSet", "title")
    val outputPath = CommandLineUtil.getParameter(args, "outputPath", "output/mapping")
    val outputType = CommandLineUtil.getParameter(args, "outputType", "json")
    val joinResultFile = CommandLineUtil.getParameter(args, "joinResultFile", "mapping")
    val overwriteOnExist = CommandLineUtil.getParameter(args, "overwriteOnExist", "false")
    val dataSet1 = new DataSetConfig(dataSet1Path, dataSet1Format, dataSet1Id,
      Option(attributes1).map(_.split(",")).orNull)
    val dataSet2 = new DataSetConfig(dataSet2Path, dataSet2Format, dataSet2Id,
      Option(attributes2).map(_.split(",")).orNull)
    log.info("dataSet1=" + dataSet1)
    log.info("dataSet2=" + dataSet2)
    def profileLoader: ProfileLoaderTrait = getProfileLoader(dataSet1.path)
    log.debug("resolve profile loader " + profileLoader)
    def includeReadIDAttr(config: DataSetConfig): Boolean = {
      config.dataSetId != null && !config.dataSetId.trim.isEmpty && config.attributes.contains(config.dataSetId)
    }
    val keepReadID1 = includeReadIDAttr(dataSet1)
    log.info("keepReadID1=" + keepReadID1)
    val profiles1 = profileLoader.load(dataSet1.path, realIDField = dataSet1.dataSetId, startIDFrom = 0, sourceId = 0, keepRealID = keepReadID1)
    val keepReadID2 = includeReadIDAttr(dataSet2)
    log.info("keepReadID2=" + keepReadID2)
    val secondEPStartID = profiles1.count().intValue()
    val profiles2 = profileLoader.load(dataSet2.path, realIDField = dataSet2.dataSetId, startIDFrom = secondEPStartID, sourceId = 1, keepRealID = keepReadID2)
    preCheckOnProfile(profiles1)
    preCheckOnProfile(profiles2)
    log.info("profiles1 first=" + profiles1.first())
    log.info("profiles2 first=" + profiles2.first())
    assert(dataSet2.path == null || dataSet1.attributes.length == dataSet2.attributes.length,
      "If dataSet 2 exist, the number of attribute use to compare between dataSet 1 and dataSet 2 should be equal")
    val attributePairsArray = collectAttributesFromProfiles(profiles1, profiles2, dataSet1, dataSet2)
    val q = options.get("q").getOrElse("2")
    val algorithm = options.get("algorithm").getOrElse(ALGORITHM_EDJOIN)
    val threshold = options.get("threshold").getOrElse("2")
    val t1 = Calendar.getInstance().getTimeInMillis
    var attributesMatches = new ArrayBuffer[RDD[(Int, Int, Double)]]()
    var attributeses = ArrayBuffer[RDD[(Int, String)]]()
    attributePairsArray.foreach(attributesTuple => {
      val attributes1 = attributesTuple._1
      val attributes2 = attributesTuple._2
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
    def intersectionMatches: RDD[(Int, Int, Double)] = {
      var matches = attributesMatches(0);
      for (i <- 1 until attributesMatches.length) {
        matches = matches.intersection(attributesMatches(i))
      }
      if (attributesMatches.length > 1) matches.cache()
      matches
    }
    val matches: RDD[(Int, Int, Double)] = intersectionMatches
    val nm = matches.count()
    log.info("[SSJoin] Number of matches " + nm)
    val t3 = Calendar.getInstance().getTimeInMillis
    attributePairsArray.foreach(attributesTuple => {
      Option(attributesTuple._1).map(_.unpersist())
      Option(attributesTuple._2).map(_.unpersist())
    })
    attributeses.foreach(_.unpersist())
    log.info("[SSJoin] Intersection time (s) " + (t3 - t2) / 1000.0)
    if (nm > 0) {
      log.info("[SSJoin] First matches " + matches.first())
    }
    val profiles = profiles1.union(profiles2)
    val clusters = ConnectedComponentsClustering.getClusters(profiles, matches.map(x => WeightedEdge(x._1, x._2, x._3)), maxProfileID = 0, edgesThreshold = 0.0)
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
    val profileMatches = mapMatchesWithProfiles(matchedPairs, profiles, secondEPStartID)
    profileMatches.take(5).foreach(x => log.info("profileMatches=" + x))
    val matchesInDiffDataSet = profileMatches.filter(t => t._1.sourceId != t._2.sourceId).zipWithIndex()
    log.info("[SSJoin] Get matched pairs " + matchesInDiffDataSet.count())
    matchesInDiffDataSet.take(5).foreach(t => {
      log.info("matches-pair=" +
        (t._2, (t._1._1.originalID, t._1._1.sourceId), (t._1._2.originalID, t._1._2.sourceId)))
    })
    val finalMap = matchesInDiffDataSet.map(x => (x._1._1.originalID, x._1._2.originalID))
    val overwriteOnExistBool = overwriteOnExist == "true" || overwriteOnExist == "1"
    val finalPath = generateOutput(finalMap, outputPath, outputType, joinResultFile, overwriteOnExistBool)
    log.info("save mapping into path " + finalPath)
    log.info("[SSJoin] Completed")
  }
  private def printContext(spark: SparkSession) = {
    log.info("-sp master=" + spark.sparkContext.master)
    log.info("-sparkContext user=" + spark.sparkContext.sparkUser)
    log.info("-sparkContext startTime=" + spark.sparkContext.startTime)
    log.info("-sparkContext appName=" + spark.sparkContext.appName)
    log.info("-sparkContext applicationId=" + spark.sparkContext.applicationId)
    log.info("-sparkContext getConf=" + spark.sparkContext.getConf)
  }
  private def generateOutput(finalMap: RDD[(String, String)], outputPath: String, outputType: String, fileName: String = "", overwrite: Boolean = false): String = {
    val spark = createLocalSparkSession(getClass.getName)
    import spark.implicits._
    val finalPath = if (fileName == null || fileName == "") {
      val inputFormat = new SimpleDateFormat("yyyy-MM-dd_HHmmss")
      outputPath + "/" + inputFormat.format(new Date()) + "-" + outputType
    } else {
      outputPath + "/" + fileName + "-" + outputType
    }
    val writer = finalMap.toDF.write
    if (overwrite) {
      writer.mode(SaveMode.Overwrite)
    }
    outputType.toLowerCase match {
      case "csv" => writer.csv(finalPath)
      case "json" => writer.json(finalPath)
      case "parquet" => writer.parquet(finalPath)
      case _ => throw new RuntimeException("Given type [" + outputType.toLowerCase + "] out of support output type")
    }
    finalPath
  }
  private def preCheckOnProfile(profiles: RDD[Profile]) = {
    val pCount = profiles.count()
    if (pCount <= 0) {
      throw new RuntimeException("Empty profile data set")
    }
    log.info("profiles count=" + pCount)
  }
  def collectAttributesFromProfiles(profiles1: RDD[Profile], profiles2: RDD[Profile], dataSet1: DataSetConfig, dataSet2: DataSetConfig): ArrayBuffer[(RDD[(Int, String)], RDD[(Int, String)])] = {
    var attributesArray = new ArrayBuffer[(RDD[(Int, String)], RDD[(Int, String)])]()
    log.info("dataSet1Attr=" + dataSet1.attributes.toList)
    log.info("dataSet2Attr=" + dataSet2.attributes.toList)
    for (i <- 0 until dataSet1.attributes.length) {
      val attributes1 = CommonFunctions.extractField(profiles1, dataSet1.attributes(i))
      val attributes2 = Option(dataSet2.attributes).map(attributes => CommonFunctions.extractField(profiles2, attributes(i))).orNull
      attributesArray += ((attributes1, attributes2))
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
  def getProfileLoader(dataFile: String): ProfileLoaderTrait = {
    ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(dataFile))
  }
}
