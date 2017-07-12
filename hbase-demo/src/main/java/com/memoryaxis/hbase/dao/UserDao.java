package com.memoryaxis.hbase.dao;

import com.memoryaxis.hbase.po.User;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.io.IOException;

/**
 * @author memoryaxis@gmail.com
 */
@Repository
public class UserDao extends HbaseDao {

    private static final byte[] TABLE_NAME = Bytes.toBytes("users");
    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("info");
    private static final byte[] USERNAME_NAME = Bytes.toBytes("username");
    private static final byte[] PASSWORD_NAME = Bytes.toBytes("password");
    private static final byte[] EMAIL_NAME = Bytes.toBytes("email");

    private static final Logger log = LoggerFactory.getLogger(UserDao.class);

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

    private Get mkGet(User user) {
        return new Get(s2b(user.getUser()));
    }

    public void insert(User user) throws IOException {
        insert(mkPut(user));
    }

    public void update(User user) throws IOException {
        update(mkPut(user));
    }

    public void delete(User user) throws IOException {
        delete(mkDelete(user));
    }

    public User get(User user) throws IOException {
        Result r = get(mkGet(user));

        User u = new User();
        u.setUsername(b2s(r.getValue(COLUMN_FAMILY_NAME, USERNAME_NAME)));
        u.setPassword(b2s(r.getValue(COLUMN_FAMILY_NAME, PASSWORD_NAME)));
        u.setEmail(b2s(r.getValue(COLUMN_FAMILY_NAME, EMAIL_NAME)));

        return user;
    }

    @Override
    TableName getTableName() {
        return TableName.valueOf(TABLE_NAME);
    }

}
