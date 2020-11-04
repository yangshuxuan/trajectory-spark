package com.zzjz.deepinsight.core.traceanalysis

import java.io.{BufferedReader, InputStreamReader}
import java.net.URI

import com.google.gson.{JsonObject, JsonParser}
import com.zzjz.deepinsight.core.traceanalysis.model.{BorderType, LineBorderType, PolygonBorderType, TrackLine, TrackPoint}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import java.sql.Timestamp

/**
 * @project zzjz-spk2.1.3hw
 * @autor 杨书轩 on 2020/6/28
 */
case class TrackCrossBorder(sparkSession:SparkSession) {
  import sparkSession.implicits._
  private val lineStringTable = "linestringtable"
  private val pointDf = "pointdf"
  implicit object TimestampOrdering extends Ordering[Timestamp] {
    override def compare(p1: Timestamp, p2: Timestamp): Int = {
      p1.compareTo(p2)
    }
  }
  def run(jsonparamStr:String,inputRDD:String => Object):DataFrame={
    val jsonParam = (new JsonParser()).parse(jsonparamStr).getAsJsonObject

    def extractPointList(pointsStr:String):List[String]={

      val hintInfo = "应遵守格式要求：先经度，后纬度，逗号隔开，每行一个点。 例如：\n" +
        List("118.684,23.416",
          "119.015,23.823",
          "119.383,24.262",
          "120.083,24.733",
          "120.708,25.537").mkString("\n")
      val ptPair = pointsStr.trim.split("\n")
      val pointPattern = raw"\s*([-+]?\d+(\.\d+)?)\s*[,，]\s*([-+]?\d+(\.\d+)?)\s*".r

      ptPair.withFilter(_.trim.nonEmpty).map{
        case pointPattern(x,_,y,_) => s"${x},${y}"
        case _ => throw new Exception(hintInfo)
      }.toList

    }
    def readHDFSFile(realUrl:String) : String= {
      //val realUrl = "hdfs://master.zzjz.com:8020/data/bank.csv"
      val hdfs = FileSystem.get(new URI(realUrl), new Configuration())
      val path = new Path(realUrl)
      val stream = new BufferedReader(new InputStreamReader(hdfs.open(path)))
      Stream.continually(stream.readLine()).takeWhile(_ != null).mkString("\n")
    }
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
    val borderTypeStr: String = extractField(jsonParam,"setIllegalPosition")

    val pointSetType= extractField(jsonParam,"borderPointSet","value")
    val points = if(pointSetType == "handInput"){
      val pointTxt = extractField(jsonParam,"borderPointSet","pointSet")
      extractPointList(pointTxt)

    }else if(pointSetType == "otherDataFrame"){

      import org.apache.spark.sql.functions.concat_ws
      val longitude = extractField(jsonParam,"borderPointSet","longitude")
      val latitude = extractField(jsonParam,"borderPointSet","latitude")
      val otherTableName = extractField(jsonParam,"borderPointSet","tableName")
      val inputDf = inputRDD(otherTableName).asInstanceOf[DataFrame]


      extractPointList(inputDf.select(concat_ws(",",col(longitude) ,col(latitude))).as[String].collect().mkString("\n"))

    }else{
      val fileName = extractField(jsonParam,"borderPointSet","getFile")
      extractPointList(readHDFSFile(fileName))
    }
    val borderType = if(borderTypeStr == "0"){
      LineBorderType
    }else{
      PolygonBorderType
    }
    val inputDf = inputRDD(tableName).asInstanceOf[DataFrame]
    val inputDs = inputDf.select(
      col(IDstr) as "trackId",
      col(time) as "timeStamp",
      col(jingdu) as "x", col(weidu) as "y").distinct().na.drop().as[TrackPoint]
    findAllCrossLines(inputDs,points,borderType)


  }
  def findAllCrossLines(input:Dataset[TrackPoint],points:List[String],borderType:BorderType):DataFrame={
      input.groupByKey(_.trackId).flatMapGroups((k,vs) => {

      val sortedVs = vs.toSeq.sortBy(_.timeStamp)
      val m = sortedVs.init zip sortedVs.tail
      m.map{case(prev,next) => TrackLine(prev,next)}

    }).createOrReplaceTempView(lineStringTable)

    sparkSession.sql(
      s"SELECT start,end,ST_LineStringFromText(linestringshape,',') AS linestringshape    FROM ${lineStringTable}"
    ).createOrReplaceTempView(pointDf)


    borderType match {
      case PolygonBorderType =>
        val region = points:+ points.head
        val regionForgis = region.map("[" + _  + "]").mkString("[",",","]")
        sparkSession.sql(s"""SELECT  start,end,"${regionForgis}" as border FROM ${pointDf} WHERE ST_Intersects(${pointDf}.linestringshape, ST_PolygonFromText('${region.mkString(",")}',','))""")
      case LineBorderType  =>
        val regionForgis = points.map("[" + _  + "]").mkString("[",",","]")
        sparkSession.sql(s"""SELECT  start,end,"${regionForgis}" as border FROM ${pointDf} WHERE ST_Intersects(${pointDf}.linestringshape, ST_LineStringFromText('${points.mkString(",")}',','))""")

    }






  }

}
