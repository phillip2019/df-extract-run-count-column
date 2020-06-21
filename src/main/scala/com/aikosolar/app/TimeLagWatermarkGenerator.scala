package com.aikosolar.app

import com.aikosolar.domain.DFTube
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark


class TimeLagWatermarkGenerator  extends AssignerWithPeriodicWatermarks[DFTube] {

  // 所有管式设备没有超过1小时的
  val maxTimeLag = 3600000L; // 1h

  override def extractTimestamp(element: DFTube, previousElementTimestamp: Long): Long = {
    element.timeSecond * 1000
  }

  override def getCurrentWatermark: Watermark = {
    // return the watermark as current time minus the maximum time lag
    new Watermark(System.currentTimeMillis() - maxTimeLag)
  }
}
