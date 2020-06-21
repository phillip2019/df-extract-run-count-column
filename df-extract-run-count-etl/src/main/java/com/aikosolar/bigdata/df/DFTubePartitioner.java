package com.aikosolar.bigdata.df;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

/**
 * @author xiaowei.song
 * @date 2020-06-21 22:03
 */
public class DFTubePartitioner extends FlinkKafkaPartitioner<DFTube> {
    @Override
    public int partition(DFTube tube, byte[] bytes, byte[] bytes1, String s, int[] ints) {
        return Integer.parseInt(tube.id);
    }
}
