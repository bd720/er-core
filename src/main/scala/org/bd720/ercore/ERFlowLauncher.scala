package org.bd720.ercore
import org.slf4j.LoggerFactory
import org.bd720.ercore.flow.{ERFlow, End2EndSimpleFlow, End2EndSimpleFlowSample, SchemaBasedSimJoinECFlow, SchemaBasedSimJoinECFlowSample}
import org.bd720.ercore.methods.util.CommandLineUtil
object ERFlowLauncher {
  val log = LoggerFactory.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
    log.info("start spark-er flow now")
    val flowType = CommandLineUtil.getParameter(args, "flowType", "SSJoin")
    val flow: ERFlow = flowType match {
      case "End2End" => End2EndSimpleFlow
      case "End2EndSample" => End2EndSimpleFlowSample
      case "SSJoinSample" => SchemaBasedSimJoinECFlowSample
      case "SSJoin" => SchemaBasedSimJoinECFlow
      case _ => throw new RuntimeException("Unsupported flow type " + flowType)
    }
    flow.run(args)
    log.info("end spark-er flow now")
  }
}
