package org.wumiguo.ser
object CallERFlowLauncher {
  def main(args: Array[String]): Unit = {
    var flowArgs = Array[String]()
    flowArgs +:= "flowType=SSJoin"
    flowArgs +:= "dataSet1=" + "/Users/mac/Development/learn/er-job/data-er-input/trade.csv"
    flowArgs +:= "dataSet1-id=" + "t_id"
    flowArgs +:= "dataSet1-format=" + "csv"
    flowArgs +:= "dataSet1-attrSet=" + "t_pid"
    flowArgs +:= "dataSet2=" + "/Users/mac/Development/learn/er-job/data-er-input/product.csv"
    flowArgs +:= "dataSet2-id=" + "p_id"
    flowArgs +:= "dataSet2-format=" + "csv"
    flowArgs +:= "dataSet2-attrSet=" + "p_id"
    flowArgs +:= "q=2"
    flowArgs +:= "threshold=2" //0,1,2
    flowArgs +:= "algorithm=EDJoin"
    flowArgs +:= "outputPath=" + "/Users/mac/Development/learn/er-spark/output/trade-product"
    flowArgs +:= "outputType=" + "csv"
    flowArgs +:= "joinResultFile=" + "aa"
    flowArgs +:= "overwriteOnExist=" + "true"
    ERFlowLauncher.main(flowArgs)
  }
}
