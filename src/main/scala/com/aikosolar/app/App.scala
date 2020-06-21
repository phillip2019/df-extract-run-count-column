package com.aikosolar.app

import java.text.SimpleDateFormat
import java.util
import java.util.Properties

import com.aikosolar.domain.DFTube
import com.aikosolar.utils.MapUtil
import com.alibaba.fastjson.JSON
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object App {
  def main(args: Array[String]): Unit = {

    // 1. 创建流式环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //2 .指定kafak相关信息
    val kafkaCluster = "172.16.111.20:9092,172.16.111.21:9092,172.16.111.22:9092"
    val kafkaTopic = "data-collection-df-test"
    // 3. 创建Kafka数据流
    val props = new Properties()
    props.setProperty("bootstrap.servers", "172.16.111.20:9092,172.16.111.21:9092,172.16.111.22:9092")
    props.setProperty("group.id", "df_test__2")
    props.setProperty("auto.commit.interval.ms", "5000")
    props.setProperty("auto.offset.reset", "earliest")
    val flinkKafkaConsumer = new FlinkKafkaConsumer010[String](kafkaTopic, new SimpleStringSchema(), props)
    flinkKafkaConsumer.setStartFromEarliest()
    //时间格式
    val sdf1 = new SimpleDateFormat("yyyyMMddHHmmssss")
    val sdf2 = new SimpleDateFormat("yyyyMMddHHmmss")
    //分区字段
    val sdf3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val sdf4 = new SimpleDateFormat("yyyyMMdd")
    //4 .设置数据源
    val kafkaDataStream: DataStream[String] = env.addSource(flinkKafkaConsumer)

    // 5. 打印数据
    val mapStream = kafkaDataStream.map(x => {
      //      val pos = x.indexOf("%")
      //      x.substring(0, if (pos == -1) x.length() else pos)

      //      StringEscapeUtils.escapeJava(x)
      /*.replaceAll("[%]", "__")
        .replaceAll("[@]", "_")
        .replaceAll("[\\[\\]/(/)]", "")*/
      x.replaceAll("""C:\\CIMConnectProjects\\CMI4\\Nvs""", "")
    })

    val jsonStream = mapStream.map(line => {
      val js = JSON.parseObject(line)
      js
    })

    val tube30sPeriodDS: DataStream[DFTube] = jsonStream.flatMap(fun = line => {
      val list: util.ArrayList[DFTube] = new util.ArrayList[DFTube](5)
      val tubePrefixMap = new util.HashMap[String, util.HashMap[String, String]](5)
      val iterator = line.keySet().iterator()
      var key: String = null
      var value: String = null
      var prefix: String = null
      var columnSuffix: String = null
      var tube2BoatIDMap = new util.HashMap[Int, String]
      while (iterator.hasNext) {
        key = iterator.next()
        value = line.getString(key)
        if (key.startsWith("Tube") && !"TubeID%String".equals(key)) {
          prefix = key.split("@")(0)
          if (!tubePrefixMap.containsKey(prefix)) {
            tubePrefixMap.put(prefix, new util.HashMap[String, String](10))
          }
          columnSuffix = key.substring(prefix.length, key.length)
          tubePrefixMap.get(prefix).put(columnSuffix, value)
        }

        // 解析管号
        if (key.startsWith("Boat") && key.contains("@Tube%String") && value.contains("-")) {
          val tubeNo = Integer.valueOf(value.split("-")(1))
          val boatPrefix = key.split("@")(0)
          val boatID = line.getString(boatPrefix + "@BoatID%String")
          tube2BoatIDMap.put(tubeNo, boatID)
        }
      }

      var dfTube: DFTube = null

      val tubePrefixIterator = tubePrefixMap.keySet().iterator()
      while (tubePrefixIterator.hasNext) {
        val key = tubePrefixIterator.next()
        val value = tubePrefixMap.get(key)
        dfTube = new DFTube
        var eqpID = line.getString("MDLN%String")
        var site: String = null
        if (StringUtils.isNotBlank(eqpID)) {
          if (eqpID.split('-').length == 2) {
            eqpID = "Z2-" + eqpID
            site = "Z2"
          }
        } else {
          site = eqpID.split("-")(0)
        }

        dfTube.eqpID = eqpID
        dfTube.site = site
        dfTube.clock = line.getOrDefault("Clock%String", "").toString
        if (StringUtils.isNotBlank(dfTube.clock)) {
          // 秒级时间

          dfTube.timeSecond = sdf1.parse(dfTube.clock).getTime / 1000
          val testTime = sdf1.parse(dfTube.clock)
          dfTube.testTime = sdf3.format(testTime)
          dfTube.ds = sdf4.format(testTime)
        }
        dfTube.recipe = line.getOrDefault("Recipe%String", "").toString

        val tubeIDStr = key.substring(4, 5)
        dfTube.tubeID = tubeIDStr
        dfTube.id = s"$eqpID-$tubeIDStr"
        dfTube.text1 = value.getOrDefault("@Memory@All@Text1%String", "")
        dfTube.text2 = value.getOrDefault("@Memory@All@Text2%String", "")
        dfTube.text3 = value.getOrDefault("@Memory@All@Text3%String", "")
        dfTube.text4 = value.getOrDefault("@Memory@All@Text4%String", "")
        dfTube.boatID = tube2BoatIDMap.getOrDefault(Integer.valueOf(tubeIDStr), "")
        dfTube.gasPOClBubbLeve = MapUtil.getValueOrDefault(value, "@Gas@POClBubb@Level%Float", "-100").toDouble
        dfTube.gasN2_POCl3VolumeAct = MapUtil.getValueOrDefault(value, "@Gas@N2_POCl3@VolumeAct%Double", "-100").toDouble
        dfTube.gasPOClBubbTempAct = MapUtil.getValueOrDefault(value, "@Gas@POClBubb@TempAct%Float", "-100").toDouble
        dfTube.dataVarAllRunCount = MapUtil.getValueOrDefault(value, "@DataVar@All@RunCount%Double", "-1").toDouble.toInt
        dfTube.dataVarAllRunNoLef = MapUtil.getValueOrDefault(value, "@DataVar@All@RunNoLef%Double", "-1").toDouble.toInt
        dfTube.vacuumDoorPressure = value.getOrDefault("@Vacuum@Door@Pressure%Float", "-1")
        dfTube.dataVarAllRunTime = value.getOrDefault("@DataVar@All@RunTime%Double", "-1")
        list.add(dfTube)
      }

      list.toArray(new Array[DFTube](0)).toSeq
    }).assignTimestampsAndWatermarks(new TimeLagWatermarkGenerator)


    var dfstream = tube30sPeriodDS
        .keyBy(_.id)
        .timeWindow(Time.hours(1), Time.minutes(5))
        .minBy("dataVarAllRunCount")
        .print()

    //jsonStream.print()
    //dfStream.print()
    /* kafkaDataStream.setParallelism(1).writeAsText("./data/sink/test",FileSystem.WriteMode.OVERWRITE)*/

    // 6.执行任务
    env.execute()
  }
}

