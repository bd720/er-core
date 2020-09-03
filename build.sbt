name := "er-spark"

organization := "org.bd720"

version := "0.1"

scalaVersion := "2.11.8"

//val scalaTestVersion = "2.2.5"
val scalaTestVersion = "3.1.2"

initialize := {
  val _ = initialize.value // run the previous initialization
  val required = "1.8"
  val current  = sys.props("java.specification.version")
  assert(current == required, s"Unsupported JDK: java.specification.version $current != $required")
}

val sparkVersion = "2.3.0"

//packageBin in Compile := file(s"${name.value}_${scalaBinaryVersion.value}.jar")


//unmanagedBase := baseDirectory.value / "custom_lib"


libraryDependencies += "org.apache.spark" % "spark-core_2.11" % sparkVersion

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.11
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % sparkVersion

// https://mvnrepository.com/artifact/org.apache.spark/spark-graphx_2.11
libraryDependencies += "org.apache.spark" % "spark-graphx_2.11" % sparkVersion

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % sparkVersion

libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % sparkVersion

// https://mvnrepository.com/artifact/com.twitter/algebird-core_2.11
//libraryDependencies += "com.twitter" % "algebird-core_2.11" % "0.12.3"

// https://mvnrepository.com/artifact/org.apache.commons/commons-math3
libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1"

// https://mvnrepository.com/artifact/commons-codec/commons-codec
libraryDependencies += "commons-codec" % "commons-codec" % "1.11"

// https://mvnrepository.com/artifact/org.jgrapht/jgrapht-core
libraryDependencies += "org.jgrapht" % "jgrapht-core" % "0.9.0"//% "1.0.1"

// https://mvnrepository.com/artifact/org.json/json
libraryDependencies += "org.json" % "json" % "20170516"

//https://www.scalatest.org
libraryDependencies += "org.scalactic" %% "scalactic" % scalaTestVersion

libraryDependencies += "org.scalatest" %% "scalatest" % scalaTestVersion % "test"

mainClass in Compile := Some("org.bd720.ercore.ERFlowLauncher")

jacocoReportSettings := JacocoReportSettings()
  .withTitle("CodeCoverage")
  .withThresholds(
    JacocoThresholds(
      instruction = 20,
      method = 24,
      branch = 14,
      complexity = 19,
      line = 21,
      clazz = 20)
  )
