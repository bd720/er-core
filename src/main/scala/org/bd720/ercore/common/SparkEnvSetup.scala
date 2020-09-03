package org.bd720.ercore.common
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
trait SparkEnvSetup {
  val log = LoggerFactory.getLogger(this.getClass.getName)
  var appConfig = scala.collection.Map[String, Any]()
  var sparkSession: SparkSession = null
  def createSparkSession(applicationName: String, appConf: SparkAppConfiguration = null): SparkSession = {
    try {
      if (sparkSession == null || sparkSession.sparkContext.isStopped) {
        val sparkConf = new SparkConf()
        sparkConf.setAppName(applicationName)
        sparkConf.setMaster(appConf.master)
        for (o <- appConf.options) {
          sparkConf.set(o._1, o._2)
          log.info("set spark config option:" + o)
        }
        val builder = SparkSession.builder().config(sparkConf)
        if (appConf.enableHiveSupport) {
          log.info("enable hive support")
          builder.enableHiveSupport()
        }
        sparkSession = builder.getOrCreate()
      }
      log.info("spark session {}", sparkSession)
      sparkSession
    }
    catch {
      case e: Exception => throw new RuntimeException("Fail to initialize spark session", e)
    }
  }
  def createLocalSparkSession(applicationName: String, configPath: String = null, outputDir: String = "/tmp/er"): SparkSession = {
    try {
      if (sparkSession == null || sparkSession.sparkContext.isStopped) {
        val sparkConf = new SparkConf()
        sparkConf.setAppName(applicationName)
          .setMaster("local[*]")
          .set("spark.default.parallelism", "4")
          .set("spark.local.dir", outputDir)
        sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
      }
      sparkSession
    }
    catch {
      case e: Exception => throw new RuntimeException("Fail to initialize spark session", e)
    }
  }
}
