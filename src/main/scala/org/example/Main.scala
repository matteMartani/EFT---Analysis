package org.example

import org.apache.spark.sql._
import org.apache.spark.sql.functions.desc

import scala.Console.println

object Main {

  def main(args: Array[String]): Unit = {

    // Set master to run locally
    val spark = SparkSession.builder()
      .appName("Dataset Visualization")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .load("src\\main\\resources\\ETFs.csv")

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


    spark.stop()
  }

}