package com.zzjz.deepinsight.core.traceanalysis.sspd

import java.sql.Timestamp

/**
 * @project zzjz-spk2.1.3hw
 * @autor 杨书轩 on 2020/7/22
 */
case class RawTrackLocation(trackId:String, timeStamp:Timestamp,Lat:Double,Long:Double)
case class TrackLocation(trackId:String, timeStamp:Timestamp, location:Location){

}
object TrackLocation{
  def apply(rawTrackLocation:RawTrackLocation):TrackLocation={
    import rawTrackLocation._
    this(trackId,timeStamp,Location(Lat,Long))
  }
}
