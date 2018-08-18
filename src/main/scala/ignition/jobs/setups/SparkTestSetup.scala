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

    println(" *** DataFrame transformed (added age, state and region) *** ")

    dfTransform.printSchema()

    dfTransform.show()

    // Creating DataFrames to optimimze queries

    println(" *** DataFrame district x customers cpf ")

    val dfDistrictCPF  : DataFrame = SparkTestJob.createDistrictCPFDF(dfTransform);

    dfDistrictCPF.printSchema()

    dfDistrictCPF.show()

    println(" *** Writing the dataframe as JSON in ./sample/target/district-cpf ")
    dfDistrictCPF.write.json(workDirectory + "/sample/target/district-cpf")

    println(" *** DataFrame state x customers cpf ")

    val dfStateCPF  : DataFrame = SparkTestJob.createStateCPFDF(dfTransform);

    dfStateCPF.printSchema()

    dfStateCPF.show()
    println(" *** Writing the dataframe as JSON in ./sample/target/state-cpf ")
    dfStateCPF.write.json(workDirectory + "/sample/target/state-cpf")

    SparkTestJob.calcHistogram(dfTransform)

  }
}
