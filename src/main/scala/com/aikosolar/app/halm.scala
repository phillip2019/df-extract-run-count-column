package com.aikosolar.app

import com.alibaba.fastjson.{JSON, JSONObject}

case class halm(
                 var Eqpid: String,
                 var TestTime: String,
                 var EndTime:String,
                 var CellIDStr: String,
                 var Comment: String,
                 var Operator: String,
                 var Title: String
               )

object halm {
  def apply(json: String): halm = {
    JSON.parseObject[halm](json, classOf[halm])
  }
}