package com.aikosolar.bigdata.df;


import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @author xiaowei.song
 */
public class TimeLagWatermarkGenerator implements AssignerWithPeriodicWatermarks<DFTube> {

    /**
     * 所有管式设备没有超过1小时的
     * 水位设置成1h，之前的数据抛弃
     **/
    public static final Long MAX_TIME_LAG = 3600000L;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(System.currentTimeMillis() - MAX_TIME_LAG);
    }

    @Override
    public long extractTimestamp(DFTube e, long l) {
        return e.timeSecond * 1000;
    }
}
