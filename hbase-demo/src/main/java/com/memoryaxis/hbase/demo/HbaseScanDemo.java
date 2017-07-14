package com.memoryaxis.hbase.demo;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * @author memoryaxis@gmail.com
 */
public class HbaseScanDemo extends HbaseDemo {

    private static final Logger log = LoggerFactory.getLogger(HbaseScanDemo.class);

    //    private static final byte[] TABLE_NAME_USERS = Bytes.toBytes("users");
    private static final String TABLE_NAME_TWITS = "twits";

    private static HbaseClient hbaseClient = HbaseClient.getInstance();

    public static void main(String[] args) throws Exception {
        HbaseScanDemo demo = new HbaseScanDemo();

//        demo.createTable();

//        demo.createCF();

//        demo.putSomeData();

//        demo.putSomeData2();

//        demo.doScan();

//        demo.doScanWithFilter();

//        demo.icv();

        demo.cap();

        hbaseClient.destory();
    }

//    private void testScan() {
//        Scan s = new Scan(s2b("i"), s2b("L"));
//    }

    /**
     * Create Table
     */
    private void createTable() throws Exception {
        Admin admin = hbaseClient.getAdmin();

        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TABLE_NAME_TWITS));
        HColumnDescriptor c = new HColumnDescriptor(TABLE_NAME_TWITS);
        c.setMaxVersions(3);
        desc.addFamily(c);

        admin.createTable(desc);
        hbaseClient.releaseAdmin(admin);
    }


    /**
     * Create Column Family
     */
    private void createCF() throws Exception {
        Admin admin = hbaseClient.getAdmin();

        HColumnDescriptor c = new HColumnDescriptor("new cf");
        c.setMaxVersions(3);
        admin.addColumn(TableName.valueOf(TABLE_NAME_TWITS), c);

        hbaseClient.releaseAdmin(admin);
    }


    /**
     * Put Any Data
     */
    private void putSomeData() throws Exception {
        Put p = new Put(s2b("LilyAllen" + System.currentTimeMillis()));
        p.addColumn(
                s2b("twits"),
                s2b("dt"),
                l2b(System.currentTimeMillis())
        );
        p.addColumn(
                s2b("twits"),
                s2b("twit"),
                s2b("Hello, TwitBase!")
        );

        Table table = hbaseClient.getTable(TABLE_NAME_TWITS);
        table.put(p);

        hbaseClient.releaseTable(table);
    }


    /**
     * Put Any Data That Flat Row-Key
     */
    private void putSomeData2() throws Exception {
        String userName = "LilyAllen";
        int longLength = Long.SIZE / 8;
        byte[] userHash = DigestUtils.md5(userName);
        byte[] timestamp = l2b(-1 * LocalDate.now().minusMonths(1).toDate().getTime());
//        byte[] timestamp = l2b(-1 * System.currentTimeMillis());
        byte[] rowKey = new byte[userHash.length + timestamp.length];
        int offset = 0;
        offset = Bytes.putBytes(rowKey, offset, userHash, 0, userHash.length);
        Bytes.putBytes(rowKey, offset, timestamp, 0, timestamp.length);

        Put put = new Put(rowKey);
        put.addColumn(
                s2b("twits"),
                s2b("dt"),
                l2b(System.currentTimeMillis())
        );
        put.addColumn(
                s2b("twits"),
                s2b("twit"),
                s2b("Bye, TwitBase!")
        );
        put.addColumn(
                s2b("twits"),
                s2b("user"),
                s2b(userName)
        );

        Table table = hbaseClient.getTable(TABLE_NAME_TWITS);
        table.put(put);

        hbaseClient.releaseTable(table);
    }


    /**
     * Basic Scan
     */
    private void doScan() throws Exception {
        int longLength = Long.SIZE / 8;
        byte[] userHash = DigestUtils.md5("LilyAllen");
        byte[] startRow = Bytes.padTail(userHash, longLength);
        byte[] stopRow = Bytes.padTail(userHash, longLength);
        stopRow[userHash.length - 1]++;

        Table table = hbaseClient.getTable(TABLE_NAME_TWITS);
        Scan s = new Scan(startRow, stopRow);

//        Filter f = new ValueFilter(
//                CompareFilter.CompareOp.EQUAL,
//                new RegexStringComparator(".*Lily.*")
//        );
//        s.setFilter(f);

        ResultScanner rs = table.getScanner(s);

        for (Result r : rs) {
            byte[] b = r.getValue(
                    s2b("twits"),
                    s2b("user")
            );
            String user = b2s(b);
            b = r.getValue(
                    s2b("twits"),
                    s2b("twit")
            );
            String message = b2s(b);

            b = Arrays.copyOfRange(
                    r.getRow(),
                    userHash.length,
                    userHash.length + longLength
            );
            DateTime dt = new DateTime(-1 * b2l(b));

            log.info("{}, {}, {}", user, message, dt.toString("yyyy-MM-dd HH:mm:ss"));
        }

        hbaseClient.releaseTable(table);
    }


    /**
     * Scan With Basic Filter
     */
    private void doScanWithFilter() throws Exception {
        int longLength = Long.SIZE / 8;
        byte[] userHash = DigestUtils.md5("LilyAllen");

        byte[] startRow = Bytes.padTail(userHash, longLength);
        byte[] stopRow = Bytes.padTail(userHash, longLength);
//        stopRow[userHash.length - 1]++;

        byte[] timestamp = l2b(-1 * LocalDate.now().minusDays(33).toDate().getTime());
        Bytes.putBytes(stopRow, userHash.length, timestamp, 0, timestamp.length);


        Table table = hbaseClient.getTable(TABLE_NAME_TWITS);
        Scan s = new Scan(startRow, stopRow);

        Filter f = new ValueFilter(
                CompareFilter.CompareOp.EQUAL,
                new RegexStringComparator(".*Lily.*")
        );
        s.setFilter(f);

        ResultScanner rs = table.getScanner(s);

        for (Result r : rs) {
            byte[] b = r.getValue(
                    s2b("twits"),
                    s2b("user")
            );
            String user = b2s(b);
            b = r.getValue(
                    s2b("twits"),
                    s2b("twit")
            );
            String message = b2s(b);

            b = Arrays.copyOfRange(
                    r.getRow(),
                    userHash.length,
                    userHash.length + longLength
            );
            DateTime dt = new DateTime(-1 * b2l(b));

            log.info("{}, {}, {}", user, message, dt.toString("yyyy-MM-dd HH:mm:ss"));
        }

        hbaseClient.releaseTable(table);
    }


    /**
     * Increment Column Value
     */
    private void icv() throws Exception {
        Table table = hbaseClient.getTable(TABLE_NAME_TWITS);
        Put put = new Put(s2b("Summary"));
        put.addColumn(
                s2b("info"),
                s2b("tweet_count"),
                l2b(10L)
        );
        table.put(put);

        long ret = table.incrementColumnValue(
                s2b("Summary"),
                s2b("info"),
                s2b("tweet_count"),
                1l
        );

        log.info("Ret: {}", ret);
        hbaseClient.releaseTable(table);
    }


    /**
     * CAP (Check And Put)
     */
    private void cap() throws Exception {
        Table table = hbaseClient.getTable(TABLE_NAME_TWITS);
        Put put = new Put(s2b("Summary"));
        put.addColumn(
                s2b("info"),
                s2b("tweet_count"),
                l2b(12L)
        );
        boolean result = table.checkAndPut(
                s2b("Summary"),
                s2b("info"),
                s2b("tweet_count"),
                l2b(11L),
                put
        );

        log.info("Is Put Success? {}", result);
        hbaseClient.releaseTable(table);
    }
}
