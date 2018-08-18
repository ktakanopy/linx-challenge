package ignition.jobs.setups

import org.apache.commons.lang3._

import ignition.jobs.SparkTestJob

import ignition.core.jobs.CoreJobRunner.RunnerContext
import ignition.core.jobs.utils.SparkContextUtils._
import ignition.core.jobs.utils.SimplePathDateExtractor.default

import org.apache.spark.sql.DataFrame 
import org.apache.spark.sql.SQLContext


object SparkTestSetup {

  val workDirectory = new java.io.File(".").getCanonicalPath 
  
  def setupStateCityRegionData(sqlContext : SQLContext) : DataFrame = { // Need to add region and state
    println("SparkTestSetup.setupStateCityRegionData()")

    val cityPath = workDirectory + "/sample/state-city/state-city.json"

    val cityDf = sqlContext.read.json(cityPath)

    cityDf
  }

  def run(runnerContext: RunnerContext) {
    println("SparkTestSetup.run()")
    val sc = runnerContext.sparkContext
    val sparkConfig = runnerContext.config
    val sqlContext = new SQLContext(sc)

    val path = workDirectory + "/sample/customers/*.json"

    val df = sqlContext.read.json(path)

    val stateCityRegionDf = setupStateCityRegionData(sqlContext)

    val dfTransform : DataFrame = SparkTestJob.transform(df, stateCityRegionDf)

    println("Final DataFrame *** ")

    dfTransform.printSchema()

    dfTransform.show()

    SparkTestJob.calcHistogram(dfTransform)

  }
}
