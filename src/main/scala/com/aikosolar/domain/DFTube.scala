package com.aikosolar.domain

/**
  * @author xiaowei.song
  * @version v1.0.0
  */
/**
  * DF 管式设备原始数据样列类
  */
class DFTube {
  var id: String = ""
  var eqpID: String = ""
  var site: String = ""
  var clock: String = ""
  var tubeID: String = ""
  var text1: String = ""
  var text2: String = ""
  var text3: String = ""
  var text4: String = ""
  var boatID: String = ""
  var gasPOClBubbLeve: Double = -100L
  var gasN2_POCl3VolumeAct: Double = 0.0D
  var gasPOClBubbTempAct: Double = 0.0D
  var recipe: String = ""
  var dataVarAllRunCount: Int = -1
  var dataVarAllRunNoLef: Int = -1
  var vacuumDoorPressure: String = ""
  var dataVarAllRunTime: String = ""
  var timeSecond: Long = -1L
  var ds: String = ""
  var testTime: String = ""
  var firstStatus: Int = 0        // 是否是第一个状态位，默认非第一个状态位


  override def toString = s"DFTube($id, $eqpID, $site, $clock, $tubeID, $text1, $text2, $text3, $text4, $boatID, $gasPOClBubbLeve, $gasN2_POCl3VolumeAct, $gasPOClBubbTempAct, $recipe, $dataVarAllRunCount, $dataVarAllRunNoLef, $vacuumDoorPressure, $dataVarAllRunTime, $timeSecond, $ds, $testTime)"
}
