package com.memoryaxis.hbase;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * @author memoryaxis@gmail.com
 */
public class HbaseDemo {

    private static final Logger log = LoggerFactory.getLogger(HbaseDemoApplication.class);

    private static final String QUORUM = "ay140718105632175cb0z";

    private static Configuration configuration;

    public static void main(String[] args) {

//        hbaseFirst();

//        hbasePool();

//        hbasePutAndUpdate();

//        hbaseGet();

        hbaseTimestamp();
    }

    static {
        configuration = HBaseConfiguration.create();
        // HBase 地址
        configuration.set(HConstants.ZOOKEEPER_QUORUM, QUORUM);
//        configuration.set("hbase.zookeeper.property.clientPort", "2181");
//        configuration.set("hbase.rpc.timeout", "3000");
//        configuration.set("zookeeper.znode.parent", "/hbase");
    }


    private static void hbaseFirst() {
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            Admin admin = connection.getAdmin();

            String usersTableName = "users";
            TableName tableName = TableName.valueOf(usersTableName);
            log.info("", admin.tableExists(tableName));
        } catch (IOException e) {
            log.error("connection hbase fail", e);
        }
    }


    private static void hbasePool() {
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            String[] tableNames = {
                    "users",
                    "mytable",
                    "mytable2",
            };

            for (String tableName : tableNames) {
                Table table = connection.getTable(TableName.valueOf(tableName));
                if (table != null) {
                    log.info("table {} is exists!", table.getName());
                    table.close();
                } else {
                    log.info("table {} is not exists!", tableName);
                }
            }
        } catch (IOException e) {
            log.error("connection hbase fail", e);
        }
    }


    private static void hbasePutAndUpdate() {
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            String tableName = "users";
            Table table = connection.getTable(TableName.valueOf(tableName));

            Put p = new Put(Bytes.toBytes("Lily Allen"));
//            p.addColumn(Bytes.toBytes("info"),
//                    Bytes.toBytes("name"),
//                    Bytes.toBytes("Lily Allen"));
//            p.addColumn(Bytes.toBytes("info"),
//                    Bytes.toBytes("account"),
//                    Bytes.toBytes("lrba0502"));
            p.addColumn(Bytes.toBytes("info"),
                    Bytes.toBytes("password"),
                    Bytes.toBytes(DigestUtils.md5Hex("lrba0502    ")));

            table.put(p);
            table.close();
        } catch (IOException e) {
            log.error("connection hbase fail", e);
        }
    }


    private static void hbaseGet() {
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            String tableName = "users";
            Table table = connection.getTable(TableName.valueOf(tableName));

            Get g = new Get(Bytes.toBytes("Lily Allen"));
//            g.addColumn(Bytes.toBytes("info"),
//                    Bytes.toBytes("password"));
            Result result = table.get(g);

            byte[] bytes = result.getValue(Bytes.toBytes("info"),
                    Bytes.toBytes("account"));
            log.info("{}", Bytes.toString(bytes));

            table.close();
        } catch (IOException e) {
            log.error("connection hbase fail", e);
//            192.168.162.111-115
        }
    }


    private static void hbaseDel() {

    }


    private static void hbaseTimestamp() {
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            String tableName = "users";
            Table table = connection.getTable(TableName.valueOf(tableName));

            Get g = new Get(Bytes.toBytes("user1"));
//            Get g = new Get(Bytes.toBytes("Lily Allen"));
            g.setMaxVersions(4);

            Result r = table.get(g);

            List<Cell> cells = r.getColumnCells(Bytes.toBytes("info"),
                    Bytes.toBytes("email"));

            for (Cell cell : cells) {
                log.info("{}, {}, {}, {}, {}",
                        Bytes.toString(r.getRow()),
                        Bytes.toString(CellUtil.cloneFamily(cell)),
                        Bytes.toString(CellUtil.cloneQualifier(cell)),
                        Bytes.toString(CellUtil.cloneValue(cell)),
                        cell.getTimestamp());
            }

            table.close();
        } catch (IOException e) {
            log.error("connection hbase fail", e);
        }
    }

}
