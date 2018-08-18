package ignition.jobs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object SparkTestJob {

    def addAgeColumn(df: DataFrame): DataFrame = { 
      println("SparkTestJob.addAgeColumn()")
      val dfTransformed = df.withColumn("age", year(current_date()) - year(to_date(df("birthDate"))) ) // add age

      dfTransformed
    }

    def calcHistogram(df: DataFrame) {
      println("SparkTestJob.calcHistogram()")
      var ageFrequencyList = df.groupBy("age").count().sort("age").rdd.map(x => (x.get(0), x.get(1) ) ).collect().toList

      val nGroup = 5

      val groupedData : List[List[(Integer,Integer)]] = ageFrequencyList.grouped(nGroup).toList.asInstanceOf[List[List[(Integer,Integer)]]]

      val nBins = ageFrequencyList.length/nGroup

      var ageFrequencyGroup : List[(Int,Int,Long)] = List()

      for( l <- groupedData ) {

        var sum = 0L; 
        var first = 0;
        var last = 0;

        for( i <- 0 until l.length ) {
          var el = l(i)
          if( i == 0 ) {
            first = el._1.asInstanceOf[scala.Int]
          } 
          if( i == l.length - 1) {
           last = el._1.asInstanceOf[scala.Int]
         }
         sum = sum + el._2.asInstanceOf[scala.Long]
       }

       ageFrequencyGroup = ageFrequencyGroup :+ (first, last,sum) 
     }

     printf("Histogram values with %d bins:\n",nBins)
     printf("Age\tFreq\n")
     for(x <- ageFrequencyGroup) {
      println(x._1 + "-" + x._2 + "\t" + x._3)
    }
    println()
  }

  def addRegionState(df: DataFrame, stateCityRegionDf: DataFrame) : DataFrame = { //add region and state
    println("SparkTestJob.addRegionState()")

    var resultDf = df.join(stateCityRegionDf, df("addresses").getField("city")(0) === stateCityRegionDf("city") && df("addresses").getField("state")(0) === stateCityRegionDf("state") ).drop("city").drop("state")
    .select(df("cpf"), df("name") , df("email"), df("gender"), df("birthDate"), df("age") , df("maritalStatus"), df("phones")
      , df("addresses.addressType"), df("addresses.city"), df("addresses.complement"), df("addresses.country")
      , stateCityRegionDf("region")
      , df("addresses.district"), df("addresses.city"), df("addresses.complement"), df("addresses.country")
      , df("addresses.number"), df("addresses.state"), df("addresses.street"), df("addresses.zipcode") 
    )

    resultDf
  }

  def transform(df: DataFrame,  stateCityRegionDf: DataFrame ): DataFrame = {
    println("SparkTestJob.transform()")
    // add age

    val dfTransformed = addAgeColumn(df)

    val dfRes = addRegionState(dfTransformed, stateCityRegionDf)
    
    dfRes
  }

  def createDistrictCPFDF(df: DataFrame) : DataFrame = {
    val dfRes = df.select(df("district"), df("cpf"))

    dfRes
  }

  def createStateCPFDF(df: DataFrame) : DataFrame = {
    val dfRes = df.select(df("state"), df("cpf"))

    dfRes
  }
}
