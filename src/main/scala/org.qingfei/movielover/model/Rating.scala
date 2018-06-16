package org.qingfei.movielover.model

import java.sql.Timestamp

/**
 * Created by ASUS on 6/14/2018.
 */
case class Rating(userId:Int,movieId:Int,rating:Double,timestamp:Long)
