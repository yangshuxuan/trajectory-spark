package com.zzjz.deepinsight.core.traceanalysis.model

/**
 * @project zzjz-spk2.1.3hw
 * @autor 杨书轩 on 2020/6/28
 */
case class TrackLine(start:TrackPoint,end:TrackPoint,linestringshape:String)
object TrackLine{
  def apply(start:TrackPoint,end:TrackPoint):TrackLine={
    TrackLine(start,end,s"${start.x},${start.y},${end.x},${end.y}")

  }
}
