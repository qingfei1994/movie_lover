package org.qingfei

import org.apache.spark.{SparkContext, SparkConf}


/**
 * Created by ASUS on 6/14/2018.
 */
object WordCount {
  def main(args: Array[String]) {
     val inputFile = "file://"
     val config = new SparkConf().setAppName("WordCount").setMaster("local")
     val sc = new SparkContext(config)
    val textFile = sc.textFile(inputFile)
    val wordCount = textFile.flatMap(line => line.split(" ")).map(word =>(word,1)).reduceByKey((a,b) => a+b)
    wordCount.foreach(println)
  }
}
