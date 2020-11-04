package com.zzjz.deepinsight.core.traceanalysis.model

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, Point}
import org.joda.time.{DateTime, DateTimeZone, LocalTime}
/**
  * @project 01src
  * @autor 杨书轩 on 2020/6/1
  */
case class GeoPointWithTime(userId:String,timeStamp:Long,x:Double,y:Double) {

}
object GeoPointWithTime{

  def apply(x:Double,y:Double):GeoPointWithTime ={
    GeoPointWithTime("bad guy",DateTime.now().getMillis(),x,y)
  }
}