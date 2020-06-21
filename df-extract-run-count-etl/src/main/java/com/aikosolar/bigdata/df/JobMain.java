package com.aikosolar.bigdata.df;

import com.aikosolar.bigdata.df.util.MapUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.Grouping;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.TimestampAssigner;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author xiaowei.song
 */
public class JobMain {

    private static final Logger logger = LoggerFactory.getLogger(JobMain.class);

    public static final String PROPERTIES_FILE_PATH = "application.properties";

    public static final SimpleDateFormat CLOCK_SDF = new SimpleDateFormat("yyyyMMddHHmmssss");
    public static final SimpleDateFormat DEFAULT_SIMPLE_SDF = new SimpleDateFormat("yyyyMMddHHmmss");
    //分区字段
    public static final SimpleDateFormat DEFAULT_SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final SimpleDateFormat DS_SDF = new SimpleDateFormat("yyyyMMdd");

    public static void main(String[] args) throws Exception {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = classLoader
                .getResourceAsStream(PROPERTIES_FILE_PATH);
        final ParameterTool parameterTool = ParameterTool.fromPropertiesFile(inputStream);

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

        AssignerWithPeriodicWatermarks<DFTube> watermarkGenerator = new TimeLagWatermarkGenerator();

        SingleOutputStreamOperator<DFTube> tube30sPeriodDS = jsonStream.flatMap((FlatMapFunction<JSONObject, DFTube>)
                (line, out) -> {
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
                dfTube.clock = line.getOrDefault("Clock%String", "").toString();
                if (StringUtils.isNotBlank(dfTube.clock)) {
                    // 秒级时间
                    dfTube.timeSecond = CLOCK_SDF.parse(dfTube.clock).getTime() / 1000;
                    Date testTime = CLOCK_SDF.parse(dfTube.clock);
                    dfTube.testTime = DEFAULT_SDF.format(testTime);
                    dfTube.ds = DS_SDF.format(testTime);
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



        SingleOutputStreamOperator<DFTube> dfstream = tube30sPeriodDS
                .returns(DFTube.class)
                .assignTimestampsAndWatermarks(watermarkGenerator)
                .keyBy(new Grouping())
                .timeWindow(Time.hours(1), Time.minutes(5))
                .minBy("dataVarAllRunCount");

        dfstream.print();
        //jsonStream.print()
        //dfStream.print()
        /* kafkaDataStream.setParallelism(1).writeAsText("./data/sink/test",FileSystem.WriteMode.OVERWRITE)*/

        // 6.执行任务
        env.execute();
    }

    public static class Grouping implements KeySelector<DFTube, Integer> {
        @Override
        public Integer getKey(DFTube tube) throws Exception {
            return Integer.valueOf(String.format("%s-%d", tube.eqpID, tube.tubeID));
        }
    }
}
