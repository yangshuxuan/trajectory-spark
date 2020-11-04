package com.zzjz.deepinsight.core.traceanalysis.sspd

/**
 * @project zzjz-spk2.1.3hw
 * @autor 杨书轩 on 2020/7/22
 */
object BasicGeographical {
  private val AVERAGE_RADIUS_OF_EARTH_KM = 6378137
  def c_great_circle_distance(startLat:Double,startLong:Double,endLat:Double,endLong:Double): Double = {
    val latDistance = Math.toRadians(startLat - endLat)
    val lngDistance = Math.toRadians(startLong - endLong)
    val sinLat = Math.sin(latDistance / 2)
    val sinLng = Math.sin(lngDistance / 2)
    val a = sinLat * sinLat +
      (Math.cos(Math.toRadians(startLat)) *
        Math.cos(Math.toRadians(endLat)) *
        sinLng * sinLng)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    (AVERAGE_RADIUS_OF_EARTH_KM * c)
  }

}
