package com.sudiyi.spark

import org.apache.spark.{SparkConf, SparkContext}


object WordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("WordCount").setMaster("local");
    val sc = new SparkContext(conf);
    val rdd = sc.textFile("spark-shell");
    val wordCount = rdd.flatMap(_.split(" "))
      .map(x => (x, 1))
      .reduceByKey(_+_)
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .map(x => (x._2, x._1));

    wordCount.saveAsTextFile("spark.text");
    sc.stop();
  }

}