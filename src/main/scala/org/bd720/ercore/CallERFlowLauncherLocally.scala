package org.bd720.ercore
import org.bd720.ercore.common.SparkEnvSetup
import org.bd720.ercore.flow.FlowOptions
import org.bd720.ercore.flow.SchemaBasedSimJoinECFlow.{createLocalSparkSession, getClass, log}
import scala.reflect.io.File
object CallERFlowLauncherLocally extends SparkEnvSetup {
  def main(args: Array[String]): Unit = {
    val outputDir: File = File("/tmp/data-er")
    if (!outputDir.exists) {
      outputDir.createDirectory(true)
    }
    val spark = createLocalSparkSession(getClass.getName, outputDir = outputDir.path)
    CallERFlowLauncher.main(args)
  }
}
