package com.zq.spark

import org.apache.spark.sql.SparkSession

object TestApp {
  def main2(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Users\\Administrator\\Downloads\\hadoop-3.3.0")
    System.setProperty("icode","A1D45B9DF9521349")
    val spark = SparkSession.builder().appName("TestApp").master("local[2]").getOrCreate()
    val rdd = spark.sparkContext.parallelize(List(1, 2, 3, 4))
    rdd.collect().foreach(println)
    spark.stop()
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Users\\Administrator\\Downloads\\hadoop-3.3.0")
    val logFile = "./qqwry.dat" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").master("local[2]").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}
