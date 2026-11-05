package com.djt.flink.sink;
import com.djt.flink.model.Offset;
import com.djt.flink.utils.Md5Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * HBase同步操作业务
 */
@Slf4j
public class HbaseSyncService implements Serializable {
    private HbaseTemplate hbaseTemplate; // HBase操作模板

    public HbaseSyncService(HbaseTemplate hbaseTemplate){
        this.hbaseTemplate = hbaseTemplate;
    }

    public void HBaseWriter(String tableName,String columnFaily,Offset offset){
        try {
            hbaseTemplate.put(tableName,columnFaily,offset);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public ResultScanner HBaseReader(String tableName){
        ResultScanner rs = hbaseTemplate.scan(tableName);
        return rs;
    }

    public void close() {
        try {
            hbaseTemplate.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 插入操作
     *
     */
    private void insert(Offset offset) {

    }

    /**
     * 获取复合字段作为rowKey的拼接
     *
     * @param rowKeyColumns 复合rowK对应的字段
     * @param data 数据
     * @return
     */
    private static String getRowKey(String[] rowKeyColumns, Map<String, String> data) {
        StringBuilder rowKeyValue = new StringBuilder();
        for (String rowKeyColumnName : rowKeyColumns) {
            Object obj = data.get(rowKeyColumnName);
            if (obj != null) {
                rowKeyValue.append(obj.toString());
            }
            rowKeyValue.append("|");
        }
        int len = rowKeyValue.length();
        if (len > 0) {
            rowKeyValue.delete(len - 1, len);
        }

        //可自行扩展支持多种rowkey生成策略，这里写死为md5前缀
        return Md5Utils.getMD5String(rowKeyValue.toString()).substring(0, 8) + "_" + rowKeyValue.toString();
    }
}
