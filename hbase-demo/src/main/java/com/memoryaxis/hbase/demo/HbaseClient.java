package com.memoryaxis.hbase.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author memoryaxis@gmail.com
 * @TODO update singleton
 */
public class HbaseClient {

    private static final Logger log = LoggerFactory.getLogger(HbaseClient.class);

    private Configuration configuration;

    private Connection connection;

    private static HbaseClient instance = null;

    private static final String QUORUM = "ay140718105632175cb0z";

    private HbaseClient() {
        init();
    }


    public void init() {
        try {
            configuration = HBaseConfiguration.create();
            configuration.set(HConstants.ZOOKEEPER_QUORUM, QUORUM);
//        configuration.set("hbase.zookeeper.property.clientPort", "2181");
//        configuration.set("hbase.rpc.timeout", "3000");
//        configuration.set("zookeeper.znode.parent", "/hbase");
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            log.error("Hbase Client Init Fail!", e);
        }
    }


    public static HbaseClient getInstance() {
        if (instance == null) {
            synchronized (HbaseClient.class) {
                if (instance == null) {
                    instance = new HbaseClient();
                }
            }
        }
        return instance;
    }


    public Table getTable(String tableName) throws Exception {
        return connection.getTable(TableName.valueOf(tableName));
    }


    public Admin getAdmin() throws Exception {
        return connection.getAdmin();
    }


    public void releaseAdmin(Admin admin) {
        if (admin == null) return;
        try {
            admin.close();
        } catch (Exception e) {
            log.error("Release HAdmin Fail!", e);
        }
    }


    public void releaseTable(Table table) {
        if (table == null) return;
        try {
            table.close();
        } catch (Exception e) {
            log.error("Release HTable Fail!", e);
        }
    }


    public void destory() {
        if (connection == null) return;
        try {
            connection.close();
        } catch (Exception e) {
            log.error("Destory Hbase Connection Fail!", e);
        }
    }

}
