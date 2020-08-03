package org.wumiguo.ser.testutil
object TestDirs {
  def testOutputDir: String = {
    val testOutputDir = "test-output"
    val url = getClass().getClassLoader.getResource(testOutputDir)
    if (url == null) {
      throw new IllegalStateException("Please make sure the test-output dir exist, if not suggest to put a place holder file in it and build")
    }
    url.getPath
  }
  def resolveOutputPath(path: String) = {
    testOutputDir + (if (path.startsWith("/")) "" else "/") + path
  }
  def testDataDir: String = {
    val testOutputDir = "data"
    getClass().getClassLoader.getResource(testOutputDir).getPath
  }
  def resolveDataPath(path: String) = {
    testDataDir + (if (path.startsWith("/")) "" else "/") + path
  }
  def resolveTestResourcePath(path: String) = {
    val url = getClass().getClassLoader.getResource(path)
    if (url == null) {
      throw new IllegalStateException("Please make sure the test resource " + path + "  exist, if not suggest to put a place holder file in it and build")
    }
    url.getPath
  }
}
