package org.bd720.ercore.methods.util
import org.apache.spark.sql.SparkSession
import org.bd720.ercore.flow.SchemaBasedSimJoinECFlow.log
object PrintContext {
  def printSparkContext() = {
    val spark = SparkSession.builder().getOrCreate()
    log.info("-sparkContext master=" + spark.sparkContext.master)
    log.info("-sparkContext user=" + spark.sparkContext.sparkUser)
    log.info("-sparkContext startTime=" + spark.sparkContext.startTime)
    log.info("-sparkContext appName=" + spark.sparkContext.appName)
    log.info("-sparkContext applicationId=" + spark.sparkContext.applicationId)
    log.info("-sparkContext getConf=" + spark.sparkContext.getConf)
    log.info("-sparkContext allConf=" + spark.sparkContext.getConf.getAll.toList)
  }
}
