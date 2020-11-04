package com.zzjz.deepinsight.core.traceanalysis

import java.io.{BufferedReader, InputStreamReader}

import com.zzjz.deepinsight.basic.BaseMain
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import com.google.gson.{Gson, JsonObject}
import com.google.gson.JsonParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI

import com.zzjz.deepinsight.core.traceanalysis.TraceanalysisMain.spark
import com.zzjz.deepinsight.core.traceanalysis.model.{BorderType, LineBorderType, PolygonBorderType, TrackPoint}
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.datasyslab.geosparkviz.sql.utils.GeoSparkVizRegistrator

/**
 * @project zzjz-spk2.1.3hw
 * @autor 杨书轩 on 2020/6/28
 */
object TrackCrossBorderMain  extends BaseMain {


  override def run(): Unit = {



    GeoSparkSQLRegistrator.registerAll(spark)
    GeoSparkVizRegistrator.registerAll(spark)



    val jsonparamStr = "<#jsonparam#>"

    val trackCrossBorder = TrackCrossBorder(spark)

    val outDf = trackCrossBorder.run(jsonparamStr,inputRDD)
    outDf.show(false)
    outputrdd.put("<#zzjzRddName#>", outDf)
    outDf.createOrReplaceTempView("<#zzjzRddName#>")


  }

}
