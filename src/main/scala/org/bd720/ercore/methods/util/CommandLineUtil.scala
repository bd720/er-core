package org.bd720.ercore.methods.util
object CommandLineUtil {
  def getParameter(args: Array[String], name: String, defaultValue: String = null): String = {
    val parameterPair = args.find(_.startsWith(name + "=")).orNull
    if (null != parameterPair)
      parameterPair.split("=")(1)
    else
      defaultValue
  }
}
