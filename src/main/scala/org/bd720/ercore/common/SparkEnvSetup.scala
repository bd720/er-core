package org.bd720.ercore.common
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
trait SparkEnvSetup {
  val log = LoggerFactory.getLogger(this.getClass.getName)
  var appConfig = scala.collection.Map[String, Any]()
  var sparkSession: SparkSession = null
  def createSparkSession(applicationName: String, configPath: String = null): SparkSession = {
    try {
      if (sparkSession == null || sparkSession.sparkContext.isStopped) {
        val sparkConf = new SparkConf()
        sparkConf.setAppName(applicationName)
        sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
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
