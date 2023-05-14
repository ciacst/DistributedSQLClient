package org.example.Raft;

import static org.apache.ratis.util.ProtoUtils.toByteString;

import java.nio.charset.Charset;


import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.example.JDBC.MySQL;

public class SQLMessage implements Message {
    public static final Charset UTF8 = Charset.forName("UTF-8");

    private final String sql;
//    private final String retMessage;

    public SQLMessage(String sql) {
        this.sql = sql;
//        this.retMessage = new String();
    }
    public SQLMessage(ByteString bytes) {
        this.sql = bytes.toStringUtf8();
    }

    @Override
    public ByteString getContent() {
        return toByteString(sql);
    }

    @Override
    public String toString() {
        return sql;
    }
}
