package ignition.jobs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object SparkTestJob {

  def addAgeColumn(df: DataFrame): DataFrame = { 
        val dfTransformed = df.withColumn("age", year(current_date()) - year(to_date(df("birthDate"))) ) // add age

        dfTransformed
  }

  def transform(df: DataFrame): DataFrame = {
        println("SparkTestJob.transform()")
        // add age

        val dfTransformed = addAgeColumn(df)

        // df.select("addresses.city").show()

        // dfTransformed.show()

        dfTransformed
     }
}
