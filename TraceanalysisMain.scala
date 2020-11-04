package com.zzjz.deepinsight.core.traceanalysis


import com.zzjz.deepinsight.basic.BaseMain


/**
  * @project 01src
  * @autor 杨书轩 on 2020/6/1
  */
object TraceanalysisMain extends BaseMain {
  override def run(): Unit = {
    import com.google.gson.{Gson, JsonObject}
    import com.zzjz.deepinsight.basic.BaseMain
    import com.google.gson.JsonParser
    import com.zzjz.deepinsight.core.traceanalysis.BaseTraceAnalysis
    import com.zzjz.deepinsight.core.traceanalysis.model.{GeoPointWithTime, UserStateInSpecifiedeTime}
    import org.apache.spark.sql.DataFrame
    import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
    import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
    import org.datasyslab.geosparkviz.sql.utils.GeoSparkVizRegistrator
    import spark.implicits._
    GeoSparkSQLRegistrator.registerAll(spark)
    GeoSparkVizRegistrator.registerAll(spark)
    val  extractTableName:String => String = (jsonParamK:String) => {
      val parser = new JsonParser()
      parser.parse(jsonParamK).getAsJsonObject.get("tableName").getAsString()
    }
    val baseTraceAnalysis:BaseTraceAnalysis = BaseTraceAnalysis(spark)
    val jsonparam = "<#zzjzParam#>"
    val tableName = extractTableName(jsonparam)
    val userStateInSpecifiedeTime = UserStateInSpecifiedeTime.transformJson(jsonparam)
    val ds = inputRDD(tableName).asInstanceOf[DataFrame].as[GeoPointWithTime]
    val updatedStateUserStateInSpecifiedeTime = baseTraceAnalysis.findAbnormalBehaviorOfSpeciedTarget(userStateInSpecifiedeTime,ds)
    val outDf = Seq(updatedStateUserStateInSpecifiedeTime.userState.toString).toDS
    outDf.show(false)
    outputrdd.put("<#zzjzRddName#>", outDf)
    outDf.createOrReplaceTempView("<#zzjzRddName#>")
  }
}