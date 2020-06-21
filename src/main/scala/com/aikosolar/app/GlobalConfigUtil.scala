package com.aikosolar.app


import com.typesafe.config.{Config, ConfigFactory}
// 配置文件加载类
object GlobalConfigUtil {

  // 通过工厂加载配置
  val config:Config = ConfigFactory.load()
  //FlinkUtils配置加载

  val checkpointDataUri:String=config.getString("checkpoint.Data.Uri")
  val bootstrapServers:String = config.getString("bootstrap.servers")
  val groupId:String = config.getString("group.id")
  val enableAutoCommit:String = config.getString("enable.auto.commit")
  val autoCommitIntervalMs: String = config.getString("auto.commit.interval.ms")
  val autoOffsetReset:String = config.getString("auto.offset.reset")
  val zookeeperConnect:String = config.getString("zookeeper.connect")
  val inputTopic:String = config.getString("input.topic")
  val outputTopic:String = config.getString("output.topic")
  //HBase的配置加载

  val tableNameStr: String = config.getString("tablenamestr")
  val tableNameStrday: String = config.getString("tablenamestrday")
  val tableNameStryear: String = config.getString("tablenamestryear")
  val columnFamilyName:String = config.getString("columnfamilyname")
  //Mysql的配置加载

  val className: String = config.getString("class.name")
  val url: String = config.getString("connection.url")
  val user: String = config.getString("connection.user")
  val password: String = config.getString("connection.password")
  val sql: String = config.getString("sql")


  def Eqpid(eqpid1:String): Int={

    val eqpid = config.getInt(eqpid1)
    eqpid
  }

  def main(args: Array[String]): Unit = {
    println(bootstrapServers)
    println(zookeeperConnect)
    println(inputTopic)
    println(groupId)
    println(enableAutoCommit)
    println(autoCommitIntervalMs)
    println(autoOffsetReset)
  }

}
