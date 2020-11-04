package com.zzjz.deepinsight.core.traceanalysis
import java.io.{BufferedReader, InputStreamReader}
import java.net.URI
import org.apache.spark.sql.functions.udf
import com.google.gson.{JsonObject, JsonParser}
import com.zzjz.deepinsight.core.traceanalysis.model.{BorderType, LineBorderType, PolygonBorderType, TrackLine, TrackPoint}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import java.sql.Timestamp
/**
 * @project zzjz-spk2.1.3hw
 * @autor 杨书轩 on 2020/7/21
 */
case class GeoPoint(Lat:Double,Long:Double,timeStamp:Timestamp,trackId:String="badboy")
case class GeoPointDistance(startLat:Double,startLong:Double,startTimeStamp:Timestamp,endLat:Double,endLong:Double,endTimeStamp:Timestamp,trackId:String="badboy",distance:Double)
case class PartDistance(endLat:Double,endLong:Double,endTimeStamp:Timestamp,distance:Double)
case class GeoPointCumDistance(startLat:Double,startLong:Double,startTimeStamp:Timestamp,trackId:String="badboy",partDistance:PartDistance)
case class GeoPointAllCumDistance(startLat:Double,startLong:Double,startTimeStamp:Timestamp,trackId:String="badboy",partDistance:PartDistance,partDistances:Seq[PartDistance])

case class TrackHoverFinder(sparkSession:SparkSession) {
  import sparkSession.implicits._

  def run(jsonparamStr:String,inputRDD:String => Object):DataFrame={
    val jsonParam = (new JsonParser()).parse(jsonparamStr).getAsJsonObject

    def extractField(jsonObject: JsonObject,fieldNames:String*):String = {
      fieldNames.toList match {
        case xh::Nil =>
          jsonObject.get(xh).getAsString
        case xh::xs =>
          extractField(jsonObject.get(xh).getAsJsonObject,xs:_*)

      }
    }

    val tableName = extractField(jsonParam,"tableName")
    val IDstr = extractField(jsonParam,"ID")
    val time = extractField(jsonParam,"time")
    val jingdu = extractField(jsonParam,"longitude")
    val weidu = extractField(jsonParam,"latitude")
    val differrate = extractField(jsonParam,"differrate").toDouble
    val leapMinutes = extractField(jsonParam,"leapMinutes").toInt
    val resultCount = extractField(jsonParam,"resultCount").toInt


    val inputDf = inputRDD(tableName).asInstanceOf[DataFrame]
    val inputDs = inputDf.select(
      col(IDstr) as "trackId",
      col(time) as "timeStamp",
      col(jingdu) as "Long", col(weidu) as "Lat").distinct().na.drop().as[GeoPoint]//.dropDuplicates("timeStamp")
    findAllHovers(inputDs,differrate,leapMinutes,resultCount)


  }
  def findAllHovers(normalIntervalSample:Dataset[GeoPoint],
                        differrate:Double = 5.0,
                        leapMinutes:Int = 10,
                        resultCount:Int = 5
                       ):DataFrame={
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._

    val round_tenths_place_udf = udf(DistanceCalcular.calculateDistanceInKilometer _)

    val starttrack = normalIntervalSample.withColumnRenamed("Lat","startLat").withColumnRenamed("Long","startLong").withColumnRenamed("timeStamp","startTimeStamp")
    val endtrack = normalIntervalSample.withColumnRenamed("Lat","endLat").withColumnRenamed("Long","endLong").withColumnRenamed("timeStamp","endTimeStamp")
    val t = starttrack.join(endtrack,"trackId").where($"endTimeStamp" > $"startTimeStamp")
    val s = t.withColumn("distance", round_tenths_place_udf($"startLat",$"startLong",$"endLat",$"endLong")).orderBy("startTimeStamp","endTimeStamp")

    val v = s.where($"startTimeStamp" =!= $"endTimeStamp").groupBy("trackId","startTimeStamp").agg(min($"endTimeStamp") as "endTimeStamp")
    val vv =v.join(s,Seq("trackId","startTimeStamp","endTimeStamp")).orderBy("startTimeStamp","endTimeStamp")
    val vvv = vv.as[GeoPointDistance].map(v => GeoPointCumDistance(v.startLat,v.startLong,v.startTimeStamp,v.trackId,PartDistance(v.endLat,v.endLong,v.endTimeStamp,v.distance)))

    val byCategoryOrderedById =
      Window.partitionBy('trackId).orderBy('startTimeStamp).rowsBetween(Window.currentRow, Window.unboundedFollowing)
    val vvvv =vvv.withColumn("partDistances", collect_list('partDistance) over byCategoryOrderedById).orderBy("startTimeStamp").as[GeoPointAllCumDistance]
    val vvvvv=vvvv.flatMap(v => {
      v.partDistances.map(e => GeoPointDistance(v.startLat,v.startLong,v.startTimeStamp,e.endLat,e.endLong,e.endTimeStamp,v.trackId,e.distance))})
    val byCategoryOrderedById2 =
      Window.partitionBy('trackId,'startTimeStamp).orderBy('endTimeStamp).rowsBetween(Window.unboundedPreceding,Window.currentRow)
    val vvvvvv =vvvvv.withColumn("accDistances", sum('distance) over byCategoryOrderedById2).orderBy("startTimeStamp","endTimeStamp")
    val hh = vvvvvv.drop("distance").join(s,Seq("trackId","startLat","startLong","startTimeStamp","endLat","endLong","endTimeStamp")).orderBy("startTimeStamp","endTimeStamp")
    val  hhh = hh.withColumn("differrate",($"accDistances" - $"distance")/$"distance").cache
    hhh.where( $"differrate" > differrate &&  $"startTimeStamp" + expr(s"INTERVAL ${leapMinutes} MINUTE") < $"endTimeStamp").orderBy($"differrate".desc).limit(resultCount)


  }

}
