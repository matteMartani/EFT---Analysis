package org.example

import org.apache.spark.sql._

object test {
  def main2(args: Array[String]): Unit = {
    println("Hello world!")
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("${NAME}").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    spark.stop()
  }

}