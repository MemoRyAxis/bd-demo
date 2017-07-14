package com.memoryaxis.hbase.demo;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author memoryaxis@gmail.com
 */
public abstract class HbaseDemo {

    protected static final String QUORUM = "ay140718105632175cb0z";

    protected byte[] s2b(String str) {
        return Bytes.toBytes(str);
    }

    protected String b2s(byte[] bytes) {
        return Bytes.toString(bytes);
    }

    protected byte[] l2b(long l) {
        return Bytes.toBytes(l);
    }

    protected long b2l(byte[] bytes) {
        return Bytes.toLong(bytes);
    }
}
