package com.zzjz.deepinsight.core.traceanalysis.model

/**
 * @project zzjz-spk2.1.3hw
 * @autor 杨书轩 on 2020/6/28
 */
sealed class BorderType()
object LineBorderType extends BorderType
object PolygonBorderType  extends BorderType
