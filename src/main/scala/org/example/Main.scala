package org.example

import org.apache.spark.sql._

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

    val explorer = new Explorer(df)
    explorer.featureExplore()
    explorer.divideByCategory(threshold = 20)

    spark.stop()
  }

}