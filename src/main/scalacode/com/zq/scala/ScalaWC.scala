package com.zq.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ScalaWC {
  def main(args: Array[String]): Unit = {
    /*// conf 可以设置SparkApplication的名称，设置spark的运行模式
    val conf = new SparkConf()
    conf.setAppName("word-count")
    conf.setMaster("local")
    // SparkContext是通往spark集群的唯一通道
    val context = new SparkContext(conf)
    val lines: RDD[String] = context.textFile("./data/data.txt")
    val data = lines.flatMap(line => {
      line.split(" ")
    })
    val pairWords: RDD[(String, Int)] = data.map(item => {
      new Tuple2(item, 1)
    })
    val result: RDD[(String, Int)] = pairWords.reduceByKey((v1: Int, v2: Int) => {
      v1 + v2
    })
    result.foreach(one=>{
      println(one)
    })
    context.stop()*/

    // 简化代码
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)
    sc.textFile("./data/data.txt").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).foreach(println)
    sc.stop()
  }
}
