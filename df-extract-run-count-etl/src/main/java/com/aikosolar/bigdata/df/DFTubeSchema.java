package com.aikosolar.bigdata.df;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

/**
 * @author xiaowei.song
 * @date 2020-06-21 21:57
 */
public class DFTubeSchema implements KeyedSerializationSchema<DFTube> {

    @Override
    public byte[] serializeKey(DFTube tube) {
        return tube.id.getBytes();
    }

    @Override
    public byte[] serializeValue(DFTube tube) {
        return JSONObject.toJSONBytes(tube);
    }

    @Override
    public String getTargetTopic(DFTube tube) {
        return null;
    }
}
