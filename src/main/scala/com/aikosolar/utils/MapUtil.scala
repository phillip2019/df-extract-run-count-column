package com.aikosolar.utils

import java.util

import com.alibaba.fastjson.JSONObject
import org.apache.commons.lang3.StringUtils

/**
  * @author xiaowei.song
  * @version v1.0.0
  */
object MapUtil {

  /**
    * 定义一个getValueOrDefault的方法
    * 从map中获取默认值，若获取为null或空字符串，则赋予默认值
    * @return
    */
  def getValueOrDefault(m: util.HashMap[String, String], key: String, defaultValue: String): String = {
    var value: String = m.get(key)
    if (StringUtils.isBlank(value)) {
      value = defaultValue
    }
    value
  }
}
