package ignition.jobs.setups


import play.api.libs.json._

import ignition.jobs.SparkTestJob

import ignition.core.jobs.CoreJobRunner.RunnerContext
import ignition.core.jobs.utils.SparkContextUtils._
import ignition.core.jobs.utils.SimplePathDateExtractor.default

import org.apache.spark.sql.DataFrame 

import scala.util.parsing.json.JSON 

object SparkTestSetup {

  val workDirectory = new java.io.File(".").getCanonicalPath 
  
  def calcHistogram(df: DataFrame) {
    println("SparkTestSetup.calcHistogram()")
  }

  def setupStateCityData() {

    val cityPath = workDirectory + "/sample/state-city/cities.json"
    val statePath = workDirectory + "/sample/state-city/states.json"

    val cityContent = scala.io.Source.fromFile(cityPath).mkString
    val stateContent = scala.io.Source.fromFile(statePath).mkString

    val statesList = JSON.parseFull(stateContent).get.asInstanceOf[List[Map[String,_]]]
    val citiesList = JSON.parseFull(cityContent).get.asInstanceOf[List[Map[String,_]]]

    val stateCitiesMap = scala.collection.mutable.Map[String, List[String]]()

    val stateIdNameMap = scala.collection.mutable.Map[Int,String]() 

    println("kljasdlkjaslkdjalskjd")
    println(statesList)
    println("kljasdlkjaslkdjalskjd")
    println(statesList.getClass.toString())
     // for(state <- statesList) {
        // println((state.getClass.toString()))
        // println(state)
        // stateIdNameMap += (state.get("id") -> state.get("abbr"))
    // }

/*    for(city <- citiesList) { 
        if(stateCitiesMap(city.state_id)) {
            stateCitiesMap(city.state_id) :+ city.name
        } else {
            stateCitiesMap(city.state_id) = List()
        }
    }

    for( (k,v) <- stateCitiesMap ) {
        println("k  = " + k)

        ls = v 
        for(l <- ls) {
            print(" " + l)
        }
    }
*/  }

  def run(runnerContext: RunnerContext) {
    println("SparkTestSetup.run()")
    val sc = runnerContext.sparkContext
    val sparkConfig = runnerContext.config
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // val lines: RDD[String] = sc.textFile(Seq("file:///home/ktakano/work/neemu/linx-challenge/sample/*.json"))

    val path = workDirectory + "/sample/customers/*.json"

    println("Path = " + path)

    val df = sqlContext.read.json(path)

    setupStateCityData()

    val dfTransform : DataFrame = SparkTestJob.transform(df)

    calcHistogram(dfTransform)

    // val top1000Words: Seq[(Int, String)] = wordCount
      // .map { case (word, count) => (count, word) }
      // .top(1000)

    // print top words
    // top1000Words.zipWithIndex.foreach { case ((freq, word), i) => println(s"${i+1}) $freq - '$word'") }
  }
}
