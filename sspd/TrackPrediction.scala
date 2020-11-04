package com.zzjz.deepinsight.core.traceanalysis.sspd
import java.sql.Timestamp

import com.google.gson.{JsonObject, JsonParser}
import org.apache.spark.ml.attribute.{Attribute, NominalAttribute}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit, log1p}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.viirya.spark.ml.AffinityPropagation
import org.apache.spark.mllib.clustering.PowerIterationClustering
/**
 * @project zzjz-spk2.1.3hw
 * @autor 杨书轩 on 2020/7/27
 */
case class TrackPrediction (sparkSession:SparkSession) {

  import sparkSession.implicits._
  val colName = "col"
  val rowName = "row"
  val distanceName = "distance"

  def extractField(jsonObject: JsonObject, fieldNames: String*): String = {
    fieldNames.toList match {
      case xh :: Nil =>
        jsonObject.get(xh).getAsString
      case xh :: xs =>
        extractField(jsonObject.get(xh).getAsJsonObject, xs: _*)

    }
  }
  def extractDistanceDF(jsonParam: JsonObject, inputRDD: String => Object):DataFrame={



    val tableName = extractField(jsonParam, "distancetableName")
    val colStr = extractField(jsonParam,colName)
    val rowStr = extractField(jsonParam, rowName)
    val distanceStr = extractField(jsonParam,distanceName)
    val inputDf = inputRDD(tableName).asInstanceOf[DataFrame]

    inputDf.select(
      col(colStr) as colName,
      col(rowStr) as rowName,
      col(distanceStr) as distanceName)

  }
  def extractDataset(prefix:String,jsonParam: JsonObject, inputRDD: String => Object):Dataset[TrackLocation]={


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

  /**
   * 采用聚类的方式选择与目标轨迹在同一cluster的轨迹点，与目标轨迹最近的若干点作为预测点
   * @param jsonparamStr
   * @param inputRDD
   * @return
   */
  def runKNNByCluster(jsonparamStr: String, inputRDD: String => Object): DataFrame = {
    val jsonParam = (new JsonParser()).parse(jsonparamStr).getAsJsonObject
    val p = extractDataset("one",jsonParam,inputRDD)
    val q = extractDistanceDF(jsonParam,inputRDD)
    val destTrackId = extractField(jsonParam,"destTrackId")
    val iterNum = extractField(jsonParam,"iterNum").toInt
    val howmanyPoints = extractField(jsonParam,"howmanyPoints").toInt
    val k = clusterByAffinityPropagation(q,destTrackId,iterNum)

    chooseTracks(p,k,destTrackId,howmanyPoints)
  }
  def runKNNByClusterPIC(jsonparamStr: String, inputRDD: String => Object): DataFrame = {
    val jsonParam = (new JsonParser()).parse(jsonparamStr).getAsJsonObject
    val p = extractDataset("one",jsonParam,inputRDD)
    val q = extractDistanceDF(jsonParam,inputRDD)
    val destTrackId = extractField(jsonParam,"destTrackId")
    val iterNum = extractField(jsonParam,"iterNum").toInt
    val howmanyPoints = extractField(jsonParam,"howmanyPoints").toInt
    val clusterNum:Int = extractField(jsonParam,"clusterNum").toInt
    val k = clusterByPIC(q,destTrackId,iterNum,clusterNum)

    chooseTracks(p,k,destTrackId,howmanyPoints)
  }
  def runKNNByNearestDistance(jsonparamStr: String, inputRDD: String => Object): DataFrame = {
    val jsonParam = (new JsonParser()).parse(jsonparamStr).getAsJsonObject
    val p = extractDataset("one",jsonParam,inputRDD)
    val q = extractDistanceDF(jsonParam,inputRDD)
    val destTrackId = extractField(jsonParam,"destTrackId")
    val knn = extractField(jsonParam,"knn").toInt
    val howmanyPoints = extractField(jsonParam,"howmanyPoints").toInt

    chooseTracksNearestDistance(p,q,destTrackId,knn,howmanyPoints)

  }
  def chooseTracks(p:Dataset[TrackLocation],q:DataFrame,destTrackId:String,howmanyPoints:Int = 5):DataFrame={
    val toUseTable = "toUseTable"
    val toUse = p.join(q,Seq("trackId"),"inner")
    val lastPoint = p.where($"trackId" === destTrackId).orderBy($"timeStamp".desc).first()
    toUse.createOrReplaceTempView(toUseTable)

    val f = sparkSession.sql(s"""SELECT trackId,timeStamp,location,ST_Transform(ST_Point(CAST(location.Long AS Decimal(24,20)), CAST(location.Lat AS Decimal(24,20))), "epsg:4326", "epsg:3857") AS pointshape from ${toUseTable}""")
    f.createOrReplaceTempView("t")

    sparkSession.sql(
      s"""
        |SELECT trackId,timeStamp,location, ST_Distance(ST_Transform(ST_Point(CAST(${lastPoint.location.Long} AS Decimal(24,20)), CAST(${lastPoint.location.Lat} AS Decimal(24,20))), "epsg:4326", "epsg:3857"), pointshape) AS distance
        |FROM t
        |ORDER BY distance ASC
  """.stripMargin).where($"distance" > 0).limit(howmanyPoints)



  }


  def clusterByPIC(dm:DataFrame,destTrackId:String,iterNum:Int = 1,clusterNum:Int = 3):DataFrame={

    val indexer = new StringIndexer().setInputCol(colName).setOutputCol("colIndex").fit(dm)
    val dmOne = indexer.transform(dm)
    val inputColSchema = dmOne.schema(indexer.getOutputCol) //临时保存colIndex的structfield，后面需要使用

    val  destTrackIdIndex= Attribute.fromStructField(inputColSchema).asInstanceOf[NominalAttribute].indexOf(destTrackId)

    indexer.setInputCol(rowName).setOutputCol("rowIndex") //对行所在的列也进行变换
    val dmTwo = indexer.transform(dmOne)

    val similarities = dmTwo.select($"colIndex",$"rowIndex", lit(1.0) / log1p(col(distanceName)/2.0)).rdd.map(v => (v(0).asInstanceOf[Double].toLong,v(1).asInstanceOf[Double].toLong,v(2).asInstanceOf[Double]))



    val model = new PowerIterationClustering()
      .setK(clusterNum)
      .setMaxIterations(iterNum)
      .setInitializationMode("degree")
      .run(similarities)


    val assign = model.assignments.filter(_.id == destTrackIdIndex).collect()
    val t  = if (assign.nonEmpty) {
      model.assignments.filter(_.cluster == assign(0).cluster).map(_.id)
    } else {
      model.assignments.sparkContext.emptyRDD[Long]
    }




    val resultRdd = t.filter(_ != destTrackIdIndex).map(v => Row(v.toDouble))
    val resultDF = sparkSession.createDataFrame(resultRdd,StructType(Seq(inputColSchema)))

    val converter = new IndexToString()
      .setInputCol("colIndex")
      .setOutputCol("trackId")

    converter.transform(resultDF)


  }
  def clusterByPICOnly(dm:DataFrame,iterNum:Int = 1,clusterNum:Int = 3):DataFrame={

    val indexer = new StringIndexer().setInputCol(colName).setOutputCol("colIndex").fit(dm)
    val dmOne = indexer.transform(dm)
    val inputColSchema = dmOne.schema(indexer.getOutputCol) //临时保存colIndex的structfield，后面需要使用



    indexer.setInputCol(rowName).setOutputCol("rowIndex") //对行所在的列也进行变换
    val dmTwo = indexer.transform(dmOne)

    //val similarities = dmTwo.select($"colIndex",$"rowIndex", lit(1.0) / log1p(col(distanceName)/2.0)).rdd.map(v => (v(0).asInstanceOf[Double].toLong,v(1).asInstanceOf[Double].toLong,v(2).asInstanceOf[Double]))

    val similarities = dmTwo.select($"colIndex",$"rowIndex", lit(1.0) / (col(distanceName)/2.0 +1)).rdd.map(v => (v(0).asInstanceOf[Double].toLong,v(1).asInstanceOf[Double].toLong,v(2).asInstanceOf[Double]))



    val model = new PowerIterationClustering()
      .setK(clusterNum)
      .setMaxIterations(iterNum)
      .setInitializationMode("degree")
      .run(similarities)





    //case class Assignment(id: Long, cluster: Int)
    val resultRdd = model.assignments.map(v=> Row(v.id.toDouble,v.cluster))
    //val resultRdd = t.filter(_ != destTrackIdIndex).map(v => Row(v.toDouble))
    val resultDF = sparkSession.createDataFrame(resultRdd,StructType(Seq(inputColSchema,StructField("clusterId",IntegerType))))

    val converter = new IndexToString()
      .setInputCol("colIndex")
      .setOutputCol("trackId")

    converter.transform(resultDF)


  }
  def clusterByAffinityPropagation(dm:DataFrame,destTrackId:String,iterNum:Int = 1):DataFrame={

    val indexer = new StringIndexer().setInputCol(colName).setOutputCol("colIndex").fit(dm)
    val dmOne = indexer.transform(dm)
    val inputColSchema = dmOne.schema(indexer.getOutputCol) //临时保存colIndex的structfield，后面需要使用

    val  destTrackIdIndex= Attribute.fromStructField(inputColSchema).asInstanceOf[NominalAttribute].indexOf(destTrackId)

    indexer.setInputCol(rowName).setOutputCol("rowIndex") //对行所在的列也进行变换
    val dmTwo = indexer.transform(dmOne)

    val similarities = dmTwo.select($"colIndex",$"rowIndex", - col(distanceName)).rdd.map(v => (v(0).asInstanceOf[Double].toLong,v(1).asInstanceOf[Double].toLong,v(2).asInstanceOf[Double]))
    val ap = new AffinityPropagation()
    val similaritiesWithPreferneces = ap.determinePreferences(similarities)
    val model = ap.setMaxIterations(iterNum).run(similaritiesWithPreferneces)



    val resultRdd = model.findCluster(destTrackIdIndex).filter(_ != destTrackIdIndex).map(v => Row(v.toDouble))
    val resultDF = sparkSession.createDataFrame(resultRdd,StructType(Seq(inputColSchema)))

    val converter = new IndexToString()
      .setInputCol("colIndex")
      .setOutputCol("trackId")

    converter.transform(resultDF)


  }
  def chooseTracksNearestDistance(p:Dataset[TrackLocation],dm:DataFrame,destTrackId:String,knn:Int = 5,howmanyPoints:Int = 5):DataFrame={
    //选择距离最近的几条轨迹
    val q = dm.where((col(colName) === destTrackId) && $"distance" =!= 0).orderBy($"distance".asc).limit(knn).select(col(rowName) as "trackId")
    val toUseTable = "toUseTable"
    val toUse = p.join(q,Seq("trackId"),"inner")
    val lastPoint = p.where($"trackId" === destTrackId).orderBy($"timeStamp".desc).first()
    toUse.createOrReplaceTempView(toUseTable)

    val f = sparkSession.sql(s"""SELECT trackId,timeStamp,location,ST_Transform(ST_Point(CAST(location.Long AS Decimal(24,20)), CAST(location.Lat AS Decimal(24,20))), "epsg:4326", "epsg:3857") AS pointshape from ${toUseTable}""")
    f.createOrReplaceTempView("t")

    sparkSession.sql(
      s"""
         |SELECT trackId,timeStamp,location, ST_Distance(ST_Transform(ST_Point(CAST(${lastPoint.location.Long} AS Decimal(24,20)), CAST(${lastPoint.location.Lat} AS Decimal(24,20))), "epsg:4326", "epsg:3857"), pointshape) AS distance
         |FROM t
         |ORDER BY distance ASC
  """.stripMargin).where($"distance" > 0).limit(howmanyPoints)

  }
}