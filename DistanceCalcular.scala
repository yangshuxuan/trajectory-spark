package com.zzjz.deepinsight.core.traceanalysis

/**
 * @project zzjz-spk2.1.3hw
 * @autor 杨书轩 on 2020/7/21
 */
object DistanceCalcular {
  private val AVERAGE_RADIUS_OF_EARTH_KM = 6371
  def calculateDistanceInKilometer(startLat:Double,startLong:Double,endLat:Double,endLong:Double): Int = {
    val latDistance = Math.toRadians(startLat - endLat)
    val lngDistance = Math.toRadians(startLong - endLong)
    val sinLat = Math.sin(latDistance / 2)
    val sinLng = Math.sin(lngDistance / 2)
    val a = sinLat * sinLat +
      (Math.cos(Math.toRadians(startLat)) *
        Math.cos(Math.toRadians(endLat)) *
        sinLng * sinLng)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    (AVERAGE_RADIUS_OF_EARTH_KM * c).toInt
  }
}