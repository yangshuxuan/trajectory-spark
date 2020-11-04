package com.zzjz.deepinsight.core.traceanalysis.sspd

import java.sql.Timestamp

import com.google.gson.{JsonObject, JsonParser}

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.col

/**
 * @project zzjz-spk2.1.3hw
 * @autor 杨书轩 on 2020/7/22
 */

case class TrackTwoLocation(trackId:String, timeStamp:Timestamp, locations:Seq[Location]) {}
case class MixTrackLocation(PointTrackId:String,PointTimeStamp:Timestamp, location:Location, LineTrackId:String,LineTimeStamp:Timestamp,locations:Seq[Location])
case class MidPointToPathDistance(pointTrackId:String,pointTimeStamp:Timestamp,lineTrackId:String,distance:Double)
case class MidResultSSPD(pointTrackId:String,lineTrackId:String,distance:Double)
case class TrackSimilityMatrixBySSPD(sparkSession:SparkSession) {

  import sparkSession.implicits._

  def extractDataset(prefix:String,jsonparamStr: String, inputRDD: String => Object):Dataset[TrackLocation]={
    val jsonParam = (new JsonParser()).parse(jsonparamStr).getAsJsonObject

    def extractField(jsonObject: JsonObject, fieldNames: String*): String = {
      fieldNames.toList match {
        case xh :: Nil =>
          jsonObject.get(xh).getAsString
        case xh :: xs =>
          extractField(jsonObject.get(xh).getAsJsonObject, xs: _*)

      }
    }
    val tableName = extractField(jsonParam, prefix + "tableName")
    val IDstr = extractField(jsonParam, prefix + "ID")
    val time = extractField(jsonParam, prefix + "time")
    val jingdu = extractField(jsonParam, prefix + "longitude")
    val weidu = extractField(jsonParam, prefix + "latitude")
    val inputDf = inputRDD(tableName).asInstanceOf[DataFrame]
    inputDf.select(
      col(IDstr) as "trackId",
      col(time) as "timeStamp",
      col(jingdu) as "Long", col(weidu) as "Lat").distinct().na.drop().as[RawTrackLocation].map(TrackLocation.apply _)
  }
  def run(jsonparamStr: String, inputRDD: String => Object): DataFrame = {
    val p = extractDataset("one",jsonparamStr,inputRDD)
    val q = extractDataset("two",jsonparamStr,inputRDD)
    computeMatrix(p,q)
  }
  def computeMatrix(a: Dataset[TrackLocation], b:Dataset[TrackLocation]
  ): DataFrame ={
    oneWaySSPD(a,b).withColumnRenamed(
      "pointTrackId","row").withColumnRenamed(
      "lineTrackId","col").withColumnRenamed(
      "distance","leftdistance").join(oneWaySSPD(b,a).withColumnRenamed(
      "pointTrackId","col").withColumnRenamed(
      "lineTrackId","row").withColumnRenamed(
      "distance","rightdistance"),Seq("col","row"),"inner").withColumn("distance",$"leftdistance" + $"rightdistance")
  }

  def oneWaySSPD(lookAsPoints: Dataset[TrackLocation],
                    lookAsLines:Dataset[TrackLocation]
                   ): Dataset[MidResultSSPD] = {
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._
    val m2rows = Window.partitionBy('trackId).orderBy('timeStamp).rowsBetween(Window.currentRow, 1)
    //.repartition(col("trackId"))
    val trackA2rows = lookAsLines.repartition(col("trackId")).withColumn("locations", collect_list('location) over m2rows).where(size($"locations") === 2).as[TrackTwoLocation]
    val f = lookAsPoints.withColumnRenamed("trackId","PointTrackId").
      withColumnRenamed("timeStamp","PointTimeStamp").crossJoin(trackA2rows.drop("location").
      withColumnRenamed("trackId","LineTrackId").
      withColumnRenamed("timeStamp","LineTimeStamp").
      withColumnRenamed("Lat","LineLat")).orderBy("PointTimeStamp","LineTimeStamp").as[MixTrackLocation]
    val g = f.map({case MixTrackLocation(pointTrackId,pointTimeStamp, location, lineTrackId,lineTimeStamp,Seq(first,second))=>
      MidPointToPathDistance(pointTrackId,pointTimeStamp,lineTrackId,first.point_to_path(second,location))})
     g.groupBy($"pointTrackId",$"pointTimeStamp",$"lineTrackId").agg(min($"distance") as "distance").groupBy($"pointTrackId",$"lineTrackId").agg(
       avg("distance") as "distance").as[MidResultSSPD]

  }
}