package org.example

import org.apache.spark.sql._

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Dataset Visualization")
      .master("local[*]") // Set master to run locally, you can change it based on your environment
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val df = spark.read
      .format("csv") // Change format based on your dataset
      .option("header", "true") // If the dataset has a header
      .load("src\\main\\resources\\ETFs.csv") // Path to your dataset

    df.printSchema()

    df.show()


    spark.stop()
  }

}