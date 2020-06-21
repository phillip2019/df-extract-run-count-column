package com.aikosolar.app

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}


object FlinkUtils {

  // 初始化flink的流式环境
  def initFlinkEnv(): StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    // 设置env的处理时间为EventTime
    /*env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //  设置并行度
    env.setParallelism(10)
    // 设置checkpointf
    // 开启checkpoint,间隔时间为5s
    env.enableCheckpointing(5000)
    // 设置处理模式
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 设置两次checkpoint的间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
    // 设置超时时长
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    // 设置并行度
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 当程序关闭的时候,触发额外的checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 设置检查点在hdfs中存储的位置
    env.setStateBackend(new FsStateBackend("hdfs://172.16.98.85:8020/flink-checkpoint"))*/
    env
  }

  def initKafkaConsumer(): FlinkKafkaConsumer010[String] ={
    // 整合Kafka
    val  props:Properties = new Properties()

    props.setProperty("bootstrap.servers",GlobalConfigUtil.bootstrapServers)
    props.setProperty("group.id",GlobalConfigUtil.groupId)
    props.setProperty("enable.auto.commit",GlobalConfigUtil.enableAutoCommit)
    props.setProperty("auto.commit.interval.ms",GlobalConfigUtil.autoCommitIntervalMs)
    props.setProperty("auto.offset.reset",GlobalConfigUtil.autoOffsetReset)


    // String topic, DeserializationSchema<T> valueDeserializer, Properties props
    val consumer = new FlinkKafkaConsumer010[String](
      GlobalConfigUtil.inputTopic,
      new SimpleStringSchema(),
      props
    )
    consumer
  }

  def initKafkaProducer(): SinkFunction[String]= {
    // 整合Kafka
    val  props:Properties = new Properties()
    props.setProperty("bootstrap.servers", GlobalConfigUtil.bootstrapServers)

    // String topic, DeserializationSchema<T> valueDeserializer, Properties props
    val producer =  new FlinkKafkaProducer010[String] (
      GlobalConfigUtil.outputTopic,
      new SimpleStringSchema,
      props
    )
    producer
  }

}
