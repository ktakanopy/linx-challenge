package ignition.jobs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object SparkTestJob {

  def addAgeColumn(df: DataFrame): DataFrame = { 
        val dfTransformed = df.withColumn("age", year(current_date()) - year(to_date(df("birthDate"))) ) // add age

        dfTransformed
  }

  def addRegionState(df: DataFrame, stateCityRegionDf: DataFrame) : DataFrame = {

      var resultDf = df.join(stateCityRegionDf, df("addresses").getField("city")(0) === stateCityRegionDf("city") && df("addresses").getField("state")(0) === stateCityRegionDf("state") ).drop("city").drop("state")
      .select(df("cpf"), df("name") , df("email"), df("gender"), df("birthDate"), df("age") , df("maritalStatus"), df("phones")
      // )
        , df("addresses.addressType"), df("addresses.city"), df("addresses.complement"), df("addresses.country")
        , stateCityRegionDf("region")
        , df("addresses.district"), df("addresses.city"), df("addresses.complement"), df("addresses.country")
        , df("addresses.number"), df("addresses.state"), df("addresses.street"), df("addresses.zipcode") 
      )

      resultDf.printSchema()
      resultDf.show(false)

      // val res = resultDf.select("city")

      resultDf
  }

  def transform(df: DataFrame,  stateCityRegionDf: DataFrame ): DataFrame = {
        println("SparkTestJob.transform()")
        // add age

        val dfTransformed = addAgeColumn(df)

        val dfRes = addRegionState(dfTransformed, stateCityRegionDf)

        // df.select("addresses.city").show()

        // dfTransformed.show()

        dfTransformed
     }
}
