package ignition.jobs.setups

import org.apache.commons.lang3._

import play.api.libs.json._

import ignition.jobs.SparkTestJob

import ignition.core.jobs.CoreJobRunner.RunnerContext
import ignition.core.jobs.utils.SparkContextUtils._
import ignition.core.jobs.utils.SimplePathDateExtractor.default

import org.apache.spark.sql.DataFrame 
import org.apache.spark.sql.SQLContext

object SparkTestSetup {

  val workDirectory = new java.io.File(".").getCanonicalPath 
  
  def calcHistogram(df: DataFrame) {
    println("SparkTestSetup.calcHistogram()")
  }

  def setupStateCityRegionData(sqlContext : SQLContext) : DataFrame = {

    val cityPath = workDirectory + "/sample/state-city/state-city.json"

    val cityDf = sqlContext.read.json(cityPath)

    cityDf.show()

    cityDf.printSchema()

    cityDf
  }

  def run(runnerContext: RunnerContext) {
    println("SparkTestSetup.run()")
    val sc = runnerContext.sparkContext
    val sparkConfig = runnerContext.config
    val sqlContext = new SQLContext(sc)

    // val lines: RDD[String] = sc.textFile(Seq("file:///home/ktakano/work/neemu/linx-challenge/sample/*.json"))

    val path = workDirectory + "/sample/customers/*.json"

    println("Path = " + path)

    val df = sqlContext.read.json(path)

    df.printSchema()

    val stateCityRegionDf = setupStateCityRegionData(sqlContext)

    val dfTransform : DataFrame = SparkTestJob.transform(df, stateCityRegionDf)

    // val top1000Words: Seq[(Int, String)] = wordCount
      // .map { case (word, count) => (count, word) }
      // .top(1000)

    // print top words
    // top1000Words.zipWithIndex.foreach { case ((freq, word), i) => println(s"${i+1}) $freq - '$word'") }
  }
}
