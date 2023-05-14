package org.example.Raft;

import static org.apache.ratis.util.ProtoUtils.toByteString;

import java.nio.charset.Charset;


import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.example.JDBC.MySQL;

public class SQLRetMessage implements Message {
    public static final Charset UTF8 = Charset.forName("UTF-8");

    private final String retMessage;

    public SQLRetMessage(String sql) {
        this.retMessage = new String();
    }
    public SQLRetMessage(ByteString bytes) {
        this.retMessage = bytes.toStringUtf8();
    }

    @Override
    public ByteString getContent() {
        return toByteString(retMessage);
    }

    @Override
    public String toString() {
        return retMessage;
    }
}
