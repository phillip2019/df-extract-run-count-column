package com.aikosolar.bigdata.df.sink;

import com.aikosolar.bigdata.df.DFTube;
import com.aikosolar.bigdata.df.client.HbaseClient;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HbaseSink<T> extends RichSinkFunction<List<T>> implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(HbaseSink.class);

    private Connection connection;
    private Admin admin;

    public HbaseSink() {
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();

        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();
    }

    public void invoke(List<T> dataList, Context context) throws Exception {
        // 按 project:table 归纳
//        Map<String, List<T>> map = new HashMap<>();
//        for (T data : dataList) {
//            if (!map.containsKey(data.getFullTable())) {
//                map.put(data.getFullTable(), new ArrayList<T>());
//            }
//            map.get(data.getFullTable()).add(data);
//        }
//        // 遍历 map
//        for(Map.Entry<String, List<T>> entry : map.entrySet()){
//            // 如果 表不存在，即创建
//            createTable(entry.getKey());
//            // 写数据
//            List<Put> list = new ArrayList<Put>();
//            for (T item : entry.getValue()) {
//                Put put = new Put(Bytes.toBytes(String.valueOf(System.currentTimeMillis())));
//
//                JSONObject object = JSONObject.parseObject(item.getData());
//                for (String key: object.keySet()) {
//                    put.addColumn("data".getBytes(), key.getBytes(), object.getString(key).getBytes());
//                }
//                list.add(put);
//            }
//            connection.getTable(TableName.valueOf(entry.getKey())).put(list);
//        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    /**
     * 创建 hbase 表
     */
    private void createTable(String tableName) throws Exception {
        createNamespace(tableName.split(":")[0]);
        TableName table = TableName.valueOf(tableName);
        if (! admin.tableExists(table)) {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(table);
            // 固定只有 data 列簇
            hTableDescriptor.addFamily(new HColumnDescriptor("data"));
            admin.createTable(hTableDescriptor);
        }
    }

    /**
     * 创建命名空间
     */
    private void createNamespace(String namespace) throws Exception {
        try {
            admin.getNamespaceDescriptor(namespace);
        } catch (NamespaceNotFoundException e) {
            admin.createNamespace(NamespaceDescriptor.create(namespace).build());
        }
    }
}