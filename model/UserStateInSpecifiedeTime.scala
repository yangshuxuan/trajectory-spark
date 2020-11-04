package com.zzjz.deepinsight.core.traceanalysis.model

import com.google.gson.{Gson, GsonBuilder, JsonDeserializationContext, JsonDeserializer, JsonElement}
import com.zzjz.deepinsight.core.traceanalysis.model.TimeDirection.TimeDirection
import com.zzjz.deepinsight.core.traceanalysis.model.userstate.{Normal, Uncertain, UserState}
import org.joda.time.{DateTime, Interval}

/**
  * @project 01src
  * @autor 杨书轩 on 2020/6/1
  */
case class UserStateInSpecifiedeTime(userId: String,
                                     interval: Interval,
                                     timeDirection: TimeDirection = TimeDirection.Ago,
                                     howManyCompares: Int = 7,
                                     userState: UserState = Uncertain("not start to find abnormal behavior")) {
  def extendInterval(): List[Interval] = {
    val end = interval.getEndMillis
    val start = interval.getStartMillis
    val duration = interval.toDurationMillis

    val t = if (timeDirection == TimeDirection.Ago) {
      -howManyCompares to -1
    } else if (timeDirection == TimeDirection.Current) {
      val mid = howManyCompares / 2 + 1
      (1 until mid) ++ (-mid to -1)

    } else {
      1 to howManyCompares
    }

    t.map(v => interval.withStartMillis(start + v * duration).withEndMillis(end + v * duration)).toList
  }
}

object UserStateInSpecifiedeTime {

  import com.google.gson.{JsonParser, JsonObject}

  import java.lang.reflect.Type


  val deserializer: JsonDeserializer[UserStateInSpecifiedeTime] = new JsonDeserializer[UserStateInSpecifiedeTime]() {

    override def deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): UserStateInSpecifiedeTime = {
      val jsonObject: JsonObject = json.getAsJsonObject();


      val start = jsonObject.get("startMills").getAsLong()
      val end = jsonObject.get("endMills").getAsLong()
      val userId = jsonObject.get("userId").getAsString()
      val interval = new Interval(start, end)
      val timeDirection:TimeDirection = TimeDirection.withName(jsonObject.get("timeDirection").getAsString())
      val howManyCompares = jsonObject.get("howManyCompares").getAsInt()
      UserStateInSpecifiedeTime(userId, interval,timeDirection, howManyCompares)

    }
  }
  def transformJson(userJson:String):UserStateInSpecifiedeTime={
    val gsonBuilder = new GsonBuilder();
    gsonBuilder.registerTypeAdapter(classOf[UserStateInSpecifiedeTime], deserializer);
    val customGson = gsonBuilder.create();
    customGson.fromJson(userJson, classOf[UserStateInSpecifiedeTime]);
  }
}
