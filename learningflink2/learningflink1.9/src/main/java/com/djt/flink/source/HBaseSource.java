package com.djt.flink.source;
import com.djt.flink.config.GlobalConfig;
import com.djt.flink.model.Offset;
import com.djt.flink.sink.HbaseSyncService;
import com.djt.flink.sink.HbaseTemplate;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.Iterator;

/**
 * @author dajiangtai
 * @create 2020-01-16-14:30
 */
public class HBaseSource extends RichSourceFunction<Offset> {

    private HbaseSyncService hbaseSyncService;

    private Offset offset=new Offset();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        org.apache.hadoop.conf.Configuration hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.set("hbase.zookeeper.quorum", GlobalConfig.HBASE_ZOOKEEPER_QUORUM);
        hbaseConfig.set("hbase.zookeeper.property.clientPort", GlobalConfig.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT);
        hbaseConfig.set("zookeeper.znode.parent", GlobalConfig.ZOOKEEPER_ZNODE_PARENT);

        HbaseTemplate hbaseTemplate = new HbaseTemplate(hbaseConfig);
        hbaseSyncService = new HbaseSyncService(hbaseTemplate);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void run(SourceContext<Offset> ctx) throws Exception  {
        ResultScanner rs = hbaseSyncService.HBaseReader(GlobalConfig.HBASE_TABLE_READER);
        Iterator<Result> iterator = rs.iterator();
        while (iterator.hasNext()) {
            Result result = iterator.next();
            String rowkey = Bytes.toString(result.getRow());
            String p0 = Bytes.toString(CellUtil.cloneValue(result
                    .getColumnLatestCell(Bytes.toBytes("offset"),
                            Bytes.toBytes("p0"))));
            String p1 = Bytes.toString(CellUtil.cloneValue(result
                    .getColumnLatestCell(Bytes.toBytes("offset"),
                            Bytes.toBytes("p1"))));
            String p2 = Bytes.toString(CellUtil.cloneValue(result
                    .getColumnLatestCell(Bytes.toBytes("offset"),
                            Bytes.toBytes("p2"))));
            offset.setP0(p0);
            offset.setP1(p1);
            offset.setP2(p2);
            offset.setRowkey(rowkey);
            ctx.collect(offset);
        }
    }

    @Override
    public void cancel() {
        hbaseSyncService.close();
    }

}
