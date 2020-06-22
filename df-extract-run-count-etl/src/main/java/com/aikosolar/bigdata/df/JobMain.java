package com.aikosolar.bigdata.df;

import com.aikosolar.bigdata.df.util.MapUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.Grouping;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.TimestampAssigner;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.sql.DataSource;
import javax.xml.crypto.Data;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author xiaowei.song
 */
public class JobMain {

    private static final Logger logger = LoggerFactory.getLogger(JobMain.class);

    public static final String PROPERTIES_FILE_PATH = "application.properties";

    public static ParameterTool parameterTool = null;

    static {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = classLoader
                .getResourceAsStream(PROPERTIES_FILE_PATH);
        try {
            parameterTool = ParameterTool.fromPropertiesFile(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("加载配置文件失败, properties file path " + PROPERTIES_FILE_PATH);
            System.exit(1);
        }

    }

    public static Map<String, Map<Integer, Long>> tubeRunCountTestTimeMap = new ConcurrentHashMap<>(6);


    public static void main(String[] args) throws Exception {
        if (parameterTool.getNumberOfParameters() < 1) {
            return;
        }

        // 1. 创建流式环境
        // 正式环境
//    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//     本地调试模式，pom文件中scope需要改为compile
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        //2 .指定kafak相关信息
        final String bootstrapServers = parameterTool.get("bootstrap.servers");
        final String etlSourceTopic = parameterTool.get("etl.source.topic");
        final String etlSourceGroupID = parameterTool.get("etl.source.group.id");
        final String etlSourceCommitIntervalMS = parameterTool.get("etl.source.auto.commit.interval.ms");
        final String etlSourceOffsetReset = parameterTool.get("etl.source.auto.offset.reset");

        final String etlTargetTopic = parameterTool.get("etl.target.topic");

        // 3. 创建Kafka数据流
        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.setProperty("bootstrap.servers", bootstrapServers);
        kafkaConsumerProps.setProperty("group.id", etlSourceGroupID);
        kafkaConsumerProps.setProperty("auto.commit.interval.ms", etlSourceCommitIntervalMS);
        kafkaConsumerProps.setProperty("auto.offset.reset", etlSourceOffsetReset);
        FlinkKafkaConsumer010<String> flinkKafkaConsumer = new FlinkKafkaConsumer010<>(
                etlSourceTopic,
                new SimpleStringSchema(),
                kafkaConsumerProps);
        flinkKafkaConsumer.setStartFromEarliest();

        //4 .设置数据源
        DataStream<String> kafkaDataStream = env.addSource(flinkKafkaConsumer);

        // 5. 打印数据
        DataStream<String> mapStream = kafkaDataStream.map(x -> {
            //      val pos = x.indexOf("%")
            //      x.substring(0, if (pos == -1) x.length() else pos)

            //      StringEscapeUtils.escapeJava(x)
            /*.replaceAll("[%]", "__")
              .replaceAll("[@]", "_")
              .replaceAll("[\\[\\]/(/)]", "")*/
            return x.replaceAll("C:\\\\CIMConnectProjects\\\\CMI4\\\\Nvs", "");
        });

        DataStream<JSONObject> jsonStream = mapStream.map(JSON::parseObject);

        SingleOutputStreamOperator<DFTube> tube30sPeriodDS = jsonStream.flatMap((FlatMapFunction<JSONObject, DFTube>)
                (line, out) -> {
                    final SimpleDateFormat clockSdf = new SimpleDateFormat("yyyyMMddHHmmssss");
                    final SimpleDateFormat defaultSimpleSdf = new SimpleDateFormat("yyyyMMddHHmmss");
                    //分区字段
                    final SimpleDateFormat defaultSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    final SimpleDateFormat dsSdf = new SimpleDateFormat("yyyyMMdd");

                    Map<String, Map<String, String>> tubePrefixMap = new HashMap<>(5);
                    Iterator iterator = line.keySet().iterator();
                    String key = null;
                    String value = null;
                    String prefix = null;
                    String columnSuffix = null;
                    Map<Integer, String> tube2BoatIDMap = new HashMap(12);
                    while (iterator.hasNext()) {
                        key = (String) iterator.next();
                        value = line.getString(key);
                        if (key.startsWith("Tube") && !"TubeID%String".equals(key)) {
                            prefix = key.split("@")[0];
                            if (!tubePrefixMap.containsKey(prefix)) {
                                tubePrefixMap.put(prefix, new HashMap<>(10));
                            }
                            columnSuffix = key.substring(prefix.length(), key.length());
                            tubePrefixMap.get(prefix).put(columnSuffix, value);
                        }

                        // 解析管号
                        if (key.startsWith("Boat") && key.contains("@Tube%String") && value.contains("-")) {
                            Integer tubeNo = Integer.valueOf(value.split("-")[1]);
                            String boatPrefix = key.split("@")[0];
                            String boatID = line.getString(boatPrefix + "@BoatID%String");
                            tube2BoatIDMap.put(tubeNo, boatID);
                        }
                    }

                    DFTube dfTube = null;
                    // 过滤非DF数据,不进行后续操作
                    if (!StringUtils.startsWith(line.getString("MDLN%String"), "DF")) {
                        logger.info("存在非DF数据，具体为{}", line.getString("MDLN%String"));
                        return;
                    }

                    Iterator tubePrefixIterator = tubePrefixMap.keySet().iterator();
                    while (tubePrefixIterator.hasNext()) {
                        String tubeKey = (String) tubePrefixIterator.next();
                        Map<String, String> tubeValueMap = tubePrefixMap.get(tubeKey);
                        dfTube = new DFTube();
                        String eqpID = line.getString("MDLN%String");
                        String site = null;
                        if (StringUtils.isNotBlank(eqpID)) {
                            if (eqpID.split("-").length == 2) {
                                eqpID = "Z2-" + eqpID;
                                site = "Z2";
                            }
                        } else {
                            site = eqpID.split("-")[0];
                        }

                        dfTube.eqpID = eqpID;
                        dfTube.site = site;
                        // 赋予默认值1970010101010100，避免无法进行窗口计算
                        dfTube.clock = line.getOrDefault("Clock%String", "1970010101010100").toString();
                        if (StringUtils.isNotBlank(dfTube.clock)) {
                            if (StringUtils.containsIgnoreCase(dfTube.clock, "E") || StringUtils.containsIgnoreCase(dfTube.clock, ".")) {
                                dfTube.clock = "1970010101010100";
                            }
                            if (!StringUtils.isNumeric(dfTube.clock)) {
                                dfTube.clock = "1970010101010100";
                            }
                            // 秒级时间
                            dfTube.timeSecond = clockSdf.parse(dfTube.clock).getTime() / 1000;
                            Date testTime = clockSdf.parse(dfTube.clock);
                            dfTube.testTime = defaultSdf.format(testTime);
                            dfTube.ds = dsSdf.format(testTime);
                        }
                        dfTube.recipe = line.getOrDefault("Recipe%String", "").toString();

                        String tubeIDStr = tubeKey.substring(4, 5);
                        dfTube.tubeID = tubeIDStr;
                        dfTube.id = String.format("%s-%s", eqpID, tubeIDStr);
                        dfTube.text1 = tubeValueMap.getOrDefault("@Memory@All@Text1%String", "");
                        dfTube.text2 = tubeValueMap.getOrDefault("@Memory@All@Text2%String", "");
                        dfTube.text3 = tubeValueMap.getOrDefault("@Memory@All@Text3%String", "");
                        dfTube.text4 = tubeValueMap.getOrDefault("@Memory@All@Text4%String", "");
                        dfTube.boatID = tube2BoatIDMap.getOrDefault(Integer.valueOf(tubeIDStr), "");
                        dfTube.gasPOClBubbLeve = Double.valueOf(MapUtil.getValueOrDefault(tubeValueMap, "@Gas@POClBubb@Level%Float", "-100"));
                        dfTube.gasN2_POCl3VolumeAct = Double.valueOf(MapUtil.getValueOrDefault(tubeValueMap, "@Gas@N2_POCl3@VolumeAct%Double", "-100"));
                        dfTube.gasPOClBubbTempAct = Double.valueOf(MapUtil.getValueOrDefault(tubeValueMap, "@Gas@POClBubb@TempAct%Float", "-100"));
                        dfTube.dataVarAllRunCount = Double.valueOf(MapUtil.getValueOrDefault(tubeValueMap, "@DataVar@All@RunCount%Double", "-1")).intValue();
                        dfTube.dataVarAllRunNoLef = Double.valueOf(MapUtil.getValueOrDefault(tubeValueMap, "@DataVar@All@RunNoLef%Double", "-1")).intValue();
                        dfTube.vacuumDoorPressure = MapUtil.getValueOrDefault(tubeValueMap, "@Vacuum@Door@Pressure%Float", "-1");
                        dfTube.dataVarAllRunTime = MapUtil.getValueOrDefault(tubeValueMap, "@DataVar@All@RunTime%Double", "-1");
                        out.collect(dfTube);
                    }
                });


        SingleOutputStreamOperator<DFTube> dfTube30PeriodStream = tube30sPeriodDS.returns(DFTube.class);

//        dfTube30PeriodStream
//                .assignTimestampsAndWatermarks(watermarkGenerator)
//                .keyBy("id")
//                .keyBy( "dataVarAllRunCount")

        AssignerWithPeriodicWatermarks<DFTube> watermarkGenerator = new TimeLagWatermarkGenerator();

        // 时间窗口计算方法
//        SingleOutputStreamOperator<DFTube> dfstream = dfTube30PeriodStream
//                .assignTimestampsAndWatermarks(watermarkGenerator)
//                .keyBy("id")
//                .keyBy("dataVarAllRunCount")
//                .timeWindow(Time.hours(1), Time.minutes(5))
//                .minBy("timeSecond")
//                .filter(new FilterFunction<DFTube>() {
//                    @Override
//                    public boolean filter(DFTube tube) throws Exception {
//                        String tubeID = tube.tubeID;
//                        if (!tubeRunCountTestTimeMap.containsKey(tubeID)) {
//                            Map<Integer, Long> enterBoatRunCountAndTestTimeMap = new ConcurrentHashMap<>(2);
//                            tubeRunCountTestTimeMap.put(tubeID, enterBoatRunCountAndTestTimeMap);
//                        }
//                        Map<Integer, Long> boatEnterTubeRunCountAndTestTimeMap = tubeRunCountTestTimeMap.get(tubeID);
//                        // TODO 清除无用的runCount信息
//                        if (!boatEnterTubeRunCountAndTestTimeMap.containsKey(tube.dataVarAllRunCount)) {
//                            boatEnterTubeRunCountAndTestTimeMap.put(tube.dataVarAllRunCount, tube.timeSecond);
//                            return true;
//                        }
//                        Long latestTimeSecond = boatEnterTubeRunCountAndTestTimeMap.get(tube.dataVarAllRunCount);
//                        // 若上次存储的入管时间小于此次入管时间，则将此信息继续传递，准备写入到kafka中
//                        if (latestTimeSecond > tube.timeSecond) {
//                            boatEnterTubeRunCountAndTestTimeMap.put(tube.dataVarAllRunCount, tube.timeSecond);
//                            return true;
//                        }
//                        return false;
//                    }
//                });

        // 计数窗口计数方案
        SingleOutputStreamOperator<DFTube> boatEnterTubeDataStream = dfTube30PeriodStream
                .assignTimestampsAndWatermarks(watermarkGenerator)
                .keyBy("id")
                .countWindow(2, 1)
                .reduce((ReduceFunction<DFTube>) (v1, v2) -> {
                    DFTube r = new DFTube();
                    if (v2.dataVarAllRunCount > v1.dataVarAllRunCount) {
                        BeanUtils.copyProperties(r, v2);
                        r.firstStatus = 1;
                    } else if (v2.dataVarAllRunCount < v1.dataVarAllRunCount) {
                        BeanUtils.copyProperties(r, v1);
                        r.firstStatus = 1;
                    } else {
                        r.firstStatus = -100;
                    }
                    return r;
                })
                .filter((FilterFunction<DFTube>) value -> value.firstStatus.equals(1) && StringUtils.isNotBlank(value.id));

//        boatEnterTubeDataStream.print();

        // 求CT
        SingleOutputStreamOperator<DFTube> boatEnterTubeCTDataStream =
                boatEnterTubeDataStream
                        .filter((FilterFunction<DFTube>) value -> value.firstStatus.equals(1) && StringUtils.isNotBlank(value.id))
                        .keyBy(DFTube::getId)
                        .window(GlobalWindows.create())
                        .apply(new WindowFunction<DFTube, DFTube, String, GlobalWindow>() {
                            @Override
                            public void apply(String s, GlobalWindow window, Iterable<DFTube> input, Collector<DFTube> out) throws Exception {
                                input.forEach(e -> {
                                    out.collect(e);
                                    System.out.println(e);
                                });
                            }
                        }).keyBy("id")
                        .countWindow(2, 1)
                        .reduce(new ReduceFunction<DFTube>() {
                            @Override
                            public DFTube reduce(DFTube v1, DFTube v2) throws Exception {
                                DFTube r = new DFTube();
                                if (Math.abs(v1.dataVarAllRunCount - v2.dataVarAllRunCount) == 1) {
                                    String testTime = v1.testTime;
                                    String runTime = v2.dataVarAllRunTime;
                                    BeanUtils.copyProperties(r, v1);
                                    if (v1.dataVarAllRunCount > v2.dataVarAllRunCount) {
                                        BeanUtils.copyProperties(r, v2);
                                        testTime = v2.testTime;
                                        runTime = v1.dataVarAllRunTime;
                                    }
                                    //分区字段
                                    final SimpleDateFormat defaultSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                    final Long runTimeSecond = Double.valueOf(runTime).longValue();
                                    r.endTime = defaultSdf.format(new Date(defaultSdf.parse(testTime).getTime() + runTimeSecond));
                                    r.ct = runTimeSecond;
                                } else if (v1.dataVarAllRunCount < v2.dataVarAllRunCount) {
                                    BeanUtils.copyProperties(r, v1);
                                    r.ct = -1L;
                                    r.endTime = "1970-01-01 01:01:00";
                                } else if (v1.dataVarAllRunCount > v2.dataVarAllRunCount) {
                                    BeanUtils.copyProperties(r, v1);
                                    r.ct = -1L;
                                    r.endTime = "1970-01-01 01:01:00";
                                } else {
                                    // 重复数据，打标后续过滤
                                    BeanUtils.copyProperties(r, v1);
                                    r.ct = -100L;
                                    r.endTime = "1970-01-01 01:01:00";
                                }
                                return r;
                            }
                        })
                        .filter((FilterFunction<DFTube>) value -> !value.ct.equals(-100L));

        // TODO 写入到kafka中，供下一步流程处理，获取最后管道状态变更信息
//        boatEnterTubeCTDataStream.print();

        final Properties kafkaProducerProps = new Properties();
        kafkaProducerProps.put("bootstrap.servers", bootstrapServers);

        boatEnterTubeCTDataStream.addSink(new FlinkKafkaProducer010<DFTube>(
                etlTargetTopic,
                new DFTubeSchema(),
                kafkaProducerProps,
                new DFTubePartitioner()
        ));
        //jsonStream.print()
        //dfStream.print()
        /* kafkaDataStream.setParallelism(1).writeAsText("./data/sink/test",FileSystem.WriteMode.OVERWRITE)*/

        logger.info("job start...");

        // 6.执行任务
        env.execute(parameterTool.get("job.name"));
    }

    public static class Grouping implements KeySelector<DFTube, Integer> {
        @Override
        public Integer getKey(DFTube tube) throws Exception {
            return Integer.valueOf(String.format("%s-%d", tube.eqpID, tube.tubeID));
        }
    }
}
