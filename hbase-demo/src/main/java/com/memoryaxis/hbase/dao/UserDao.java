package com.memoryaxis.hbase.dao;

import com.memoryaxis.hbase.po.User;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * @author memoryaxis@gmail.com
 */
public class UserDao {

    private static final byte[] TABLE_NAME = Bytes.toBytes("users");
    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("info");
    private static final byte[] USERNAME_NAME = Bytes.toBytes("username");
    private static final byte[] PASSWORD_NAME = Bytes.toBytes("password");
    private static final byte[] EMAIL_NAME = Bytes.toBytes("email");

    private static final Logger log = LoggerFactory.getLogger(UserDao.class);

    private static Table table;

    private void init() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set(HConstants.ZOOKEEPER_QUORUM, "AY140718105632175cb0Z");
        try (
                Connection connection = ConnectionFactory.createConnection(configuration)
        ) {
            table = connection.getTable(TableName.valueOf(TABLE_NAME));
        } catch (IOException e) {
            log.error("Init HBase Fail!", e);
        }
    }

    private byte[] s2b(String str) {
        return Bytes.toBytes(str);
    }

    private Put mkPut(User user) {
        Put p = new Put(s2b(user.getUser()));
        p.addColumn(COLUMN_FAMILY_NAME, USERNAME_NAME, s2b(user.getUsername()));
        p.addColumn(COLUMN_FAMILY_NAME, PASSWORD_NAME, s2b(user.getPassword()));
        p.addColumn(COLUMN_FAMILY_NAME, EMAIL_NAME, s2b(user.getEmail()));
        return p;
    }

    private Delete mkDelete(User user) {
        Delete d = new Delete(s2b(user.getUser()));
        d.addColumn(COLUMN_FAMILY_NAME, USERNAME_NAME);
        d.addColumn(COLUMN_FAMILY_NAME, PASSWORD_NAME);
        d.addColumn(COLUMN_FAMILY_NAME, EMAIL_NAME);
        return d;
    }

    public void insert(User user) throws IOException {
        table.put(mkPut(user));
    }

    public void update(User user) throws IOException {
        table.put(mkPut(user));
    }

    public void delete(User user) throws IOException {
        table.delete(mkDelete(user));
    }

    public List<User> getList(User user) throws IOException {
        return null;
    }
}
