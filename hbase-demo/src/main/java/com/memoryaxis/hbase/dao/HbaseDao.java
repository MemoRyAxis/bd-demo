package com.memoryaxis.hbase.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author memoryaxis@gmail.com
 */
public abstract class HbaseDao {

    private static final Logger log = LoggerFactory.getLogger(HbaseDao.class);

    private Connection connection;

    private Table table;

    private void init() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set(HConstants.ZOOKEEPER_QUORUM, "AY140718105632175cb0Z");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            table = connection.getTable(getTableName());
        } catch (IOException e) {
            log.error("Init HBase Fail!", e);
        }
    }

    protected byte[] s2b(String str) {
        return Bytes.toBytes(str);
    }

    protected String b2s(byte[] bytes) {
        return Bytes.toString(bytes);
    }

    protected void insert(Put p) throws IOException {
        init();
        table.put(p);
    }

    protected void update(Put p) throws IOException {
        init();
        table.put(p);
    }

    protected void delete(Delete d) throws IOException {
        init();
        table.delete(d);
    }

    protected Result get(Get g) throws IOException {
        init();
        return table.get(g);
    }

    abstract TableName getTableName();

}
