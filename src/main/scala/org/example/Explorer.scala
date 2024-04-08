package org.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.NumericType
import org.apache.spark.sql.functions.col


import scala.Console.println

class Explorer(df: DataFrame) {

  def featureExplore(): Unit = {

    println("Number of total rows in the dataset:\n")
    println(df.count())

    println("Let's start by analyzing the ETF Table \n")
    println("Schema:")
    df.printSchema()

    println("The table itself looks like this: ")
    df.show(truncate = false)

    println("The total number of rows is: " + df.count())

    println("Now let's dive deep in some features\n")
    df.groupBy("region").count().show()
    df.groupBy("currency").count().show()
    df.groupBy("fund_category").count().orderBy(desc("count")).show(truncate = false)
    df.groupBy("fund_family").count().orderBy(desc("count")).show(truncate = false)
    df.groupBy("exchange_name").count().show()
    df.groupBy("investment_type").count().show()
    df.groupBy("size_type").count().show()
    df.groupBy("fund_annual_report_net_expense_ratio").count().show()
    df.groupBy("asset_stocks").count().orderBy(desc("count")).show()
    df.orderBy(desc("fund_return_2020")).show()
  }

  // Function to check if a string can be converted to a numeric type
  def isNumeric(value: String): Boolean = {
    try {
      value.toDouble
      true
    } catch {
      case _: NumberFormatException => false
    }
  }

  // Function to determine if a column contains numeric values (either discrete or continuous)
  private def isColumnNumeric(colName: String): Boolean = {
    val nonNullValues = df.select(colName).na.drop().collect().map(_.getString(0))
    nonNullValues.forall(isNumeric)
  }

  private def isNumericDiscrete(col: String, threshold: Int): Boolean = {
    val countDistinct = df.select(col).distinct().count()
    countDistinct < df.count() / threshold
  }

  def divideByCategory(threshold: Int): Unit = {
    val columnCategories = df.schema.fields.map { field =>
      val colName = field.name
      val colType = field.dataType

      if (colType.isInstanceOf[NumericType] || isColumnNumeric(colName)) {
        val isDiscrete = isNumericDiscrete(colName, threshold)
        if (isDiscrete) {
          (colName, "Numerical Discrete")
        } else {
          (colName, "Numerical Continuous")
        }
      } else {
        (colName, "Qualitative")
      }
    }

    val numNumericDiscrete = columnCategories.count(_._2 == "Numerical Discrete")
    val numNumericContinuous = columnCategories.count(_._2 == "Numerical Continuous")
    val numQualitative = columnCategories.count(_._2 == "Qualitative")

    println(s"Number of Numerical Discrete Features: $numNumericDiscrete")
    println(s"Number of Numerical Continuous Features: $numNumericContinuous")
    println(s"Number of Qualitative Features: $numQualitative")
  }

}