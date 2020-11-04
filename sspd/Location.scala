package com.zzjz.deepinsight.core.traceanalysis.sspd

/**
 * @project zzjz-spk2.1.3hw
 * @autor 杨书轩 on 2020/7/22
 */
case class Location(Lat:Double,Long:Double) {
  private final val AVERAGE_RADIUS_OF_EARTH_KM = 6378137
  private final val R = AVERAGE_RADIUS_OF_EARTH_KM
  val rad:Double = math.Pi / 180.0
  def initial_bearing(that:Location):Double = {

    val lon2 = that.Long
    val lon1 = this.Long
    val lat2 = that.Lat
    val lat1 = this.Lat

    val dLon = rad*(lon2 - lon1)
    val y = math.sin(dLon) * math.cos(rad*(lat2))
    val x = math.cos(rad*(lat1))*math.sin(rad*(lat2)) - math.sin(rad*(lat1))*math.cos(rad*(lat2))*math.cos(dLon)
    math.atan2(y, x)
  }
  def great_circle_distance(that:Location):Double = {
    val startLat = this.Lat
    val endLat = that.Lat
    val startLong = this.Long
    val endLong = that.Long
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
  def cross_track_distance(that:Location,other:Location):Double = {

    val d13 = this.great_circle_distance(other)

    val theta13 = this.initial_bearing(other)

    val theta12 = this.initial_bearing(that)


    math.asin(math.sin(d13 / AVERAGE_RADIUS_OF_EARTH_KM) * math.sin(theta13 - theta12)) * AVERAGE_RADIUS_OF_EARTH_KM


  }
  def along_track_distance(crt:Double,that:Location):Double = {

    val d13 = this.great_circle_distance(that)

    math.acos(math.cos(d13 / R) / math.cos(crt / R)) * R
  }
  def point_to_path(that:Location,other:Location):Double= {
    val crt = this.cross_track_distance(that,other)
    val d1p = this.along_track_distance(crt,other)
    val d2p = that.along_track_distance(crt, other)
    val d12 = this.great_circle_distance(that)
    if ((d1p > d12) || (d2p > d12))
      math.min(this.great_circle_distance(other), that.great_circle_distance(other))
    else
      math.abs(crt)

  }

}

