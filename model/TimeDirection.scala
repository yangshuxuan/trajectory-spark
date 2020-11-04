package com.zzjz.deepinsight.core.traceanalysis.model
/**
  * @project 01src
  * @autor 杨书轩 on 2020/6/1
  */
object TimeDirection  extends Enumeration {
  type TimeDirection = Value



  val Ago = Value("ago")
  val Current   = Value("current")
  val Future   = Value("future")

}