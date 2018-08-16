package ignition.jobs.setups


import ignition.jobs.SparkTestJob

import ignition.core.jobs.CoreJobRunner.RunnerContext
import ignition.core.jobs.utils.SparkContextUtils._
import ignition.core.jobs.utils.SimplePathDateExtractor.default

import org.apache.spark.sql.DataFrame 

object SparkTestSetup {

  def calcHistogram(df: DataFrame) {
    println("SparkTestSetup.calcHistogram()")
  }

  def run(runnerContext: RunnerContext) {
    println("SparkTestSetup.run()")
    val workDirectory = new java.io.File(".").getCanonicalPath 
    val sc = runnerContext.sparkContext
    val sparkConfig = runnerContext.config
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // val lines: RDD[String] = sc.textFile(Seq("file:///home/ktakano/work/neemu/linx-challenge/sample/*.json"))

    val path = workDirectory + "/sample/*.json"

    println("Path = " + path)

    val df = sqlContext.read.json(path)

    // The job is generic and can be used in other contexts
    // This setup is specific because it binds a certain source of files (gutenberg books)
    // to a certain output (top 1000 words printed in console)
    // val wordCount: RDD[(String, Int)] = WordCountJob.wc(lines)

    val dfTransform : DataFrame = SparkTestJob.transform(df)

    calcHistogram(dfTransform)

    // val top1000Words: Seq[(Int, String)] = wordCount
      // .map { case (word, count) => (count, word) }
      // .top(1000)

    // print top words
    // top1000Words.zipWithIndex.foreach { case ((freq, word), i) => println(s"${i+1}) $freq - '$word'") }
  }
}
