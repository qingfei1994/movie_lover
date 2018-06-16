package org.qingfei.movielover.preprocess

import java.sql.Timestamp

import org.apache.spark
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.{SparkContext, SparkConf}
import org.qingfei.movielover.model.{Tag, Rating, Movie, Link}
import org.apache.spark.sql.{SaveMode, SparkSession}


/**
 * Created by ASUS on 6/14/2018.
 */
object Preprocessor {
  def main(args: Array[String]) {
    val config = new SparkConf().setAppName("Preprocessor").setMaster("local")
    val sparkContext = new SparkContext(config)
    val sparkSession = SparkSession.builder.config(config).enableHiveSupport.getOrCreate()
    val linkFile = "file:///usr/local/ml-latest-small/links.csv"
    val movieFile = "file:///usr/local/ml-latest-small/movies.csv"
    val ratingFile = "file:///usr/local/ml-latest-small/ratings.csv"
    val tagsFile = "file:///usr/local/ml-latest-small/tags.csv"
    import sparkSession.implicits._
    val links = sparkContext.textFile(linkFile)
      //The last column may be null
      .filter(!_.endsWith(","))
      //去掉第一行
      .mapPartitionsWithIndex{(index,iter)=> if(index==0) iter.drop(1) else iter}
      .map(line => line.split(","))
      .map(array => Link(array(0).trim.toInt,array(1).trim.toInt,array(2).trim.toInt)).toDF


    val movies = sparkContext.textFile(movieFile)
     .mapPartitionsWithIndex{(index,iter) => if(index==0) iter.drop(1) else iter}
     .map(line => line.split(","))
    .map(array => Movie(array(0).trim().toInt,array(1),array(2))).toDF


    val ratings = sparkContext.textFile(ratingFile)
    .mapPartitionsWithIndex{(index,iter) => if(index==0) iter.drop(1) else iter}
    .map(line => line.split(","))
    .map(array => Rating(array(0).trim().toInt,array(1).trim().toInt,array(2).trim.toDouble,array(3).trim.toLong)).toDF


    val tags = sparkContext.textFile(tagsFile)
    .mapPartitionsWithIndex{(index,iter) => if(index==0) iter.drop(1) else iter}
    .map(line => line.split(","))
    .map(array => Tag(array(0).trim.toInt,array(1).trim.toInt,array(2),array(3).toLong)).toDF

    //persist into hive tables

    import sparkSession.sql
    sql("create table if not exists link(movieId INT, imdbId INT,tmdbId INT) stored as ORC")
    links.write.mode(SaveMode.Overwrite).saveAsTable("link")
    sql("select * from link limit 10").show

    sql("create table if not exists movie(movieId Int, title String, genres String) stored as ORC")
    movies.write.mode(SaveMode.Overwrite).saveAsTable("movie")
    sql("select * from movie limit 5").show

    sql("create table if not exists rating(userId Int,movieId Int,rating Double,timestamp Long) stored as ORC")
    ratings.write.mode(SaveMode.Overwrite).saveAsTable("rating")
    sql("select * from rating limit 5")

    sql("create table if not exists tag(userId Int,movieId Int,tag String,timestamp:Long) stored as ORC")
    tags.write.mode(SaveMode.Overwrite).saveAsTable("tag")
    sql("select * from tag limit 5")

  }
  /*Tag表中数据大部分是这样的：15,1955,dentist,1193435061
  *  但是也有一部分的数据是这样的:402,260,"space epic, science fiction, hero's journey",1443393664
  *  需要对这部分数据进行清洗,思路是去掉双引号和中间的逗号，因为这些标点符号都是没有意义的。
  */
  def cleanTag(input:String):String = {
    val array = input.split(",")
    //head:402,260
    val head = array.take(2).mkString(",")
    //tail是从右边开始拿一个元素:1443393664
    val tail = array.takeRight(1).mkString
    //others是去掉最后一个元素和去掉前两个元素:"space epic science fiction hero's journey"
    val middle = array.dropRight(1).drop(2).mkString
    //最后拼在一起，并且将双引号替换掉
    s"$head,$middle,$tail".replaceAll("\"","")

  }

}
