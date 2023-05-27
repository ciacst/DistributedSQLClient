package org.example.Raft;

import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.StateMachine;
//import org.example.BaseStateMachine;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.AutoCloseableLock;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.MD5FileUtil;
import org.example.JDBC.MySQL;
import org.example.api.MasterRegionServiceImpl;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.example.region.RegionServer.service;

public class SQLStateMachine extends BaseStateMachine {
    private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final MySQL SQLStorage = new MySQL();

    private AutoCloseableLock writeLock() {
        return AutoCloseableLock.acquire(lock.writeLock());
    }

    @Override
    public void initialize(RaftServer server, RaftGroupId groupId,
                           RaftStorage raftStorage) throws IOException {
        super.initialize(server, groupId, raftStorage);
        this.storage.init(raftStorage);
        loadSnapshot(storage.getLatestSnapshot());
    }

    @Override
    public void reinitialize() throws IOException {
        close();
        loadSnapshot(storage.getLatestSnapshot());
    }

    @Override
    public long takeSnapshot() {
        final TermIndex last;
        try(AutoCloseableLock writeLock = writeLock()) {
            last = getLastAppliedTermIndex();
        }

        final File snapshotFile =  storage.getSnapshotFile(last.getTerm(), last.getIndex());
        LOG.info("Taking a snapshot to file {}", snapshotFile);

        final MD5Hash md5 = MD5FileUtil.computeAndSaveMd5ForFile(snapshotFile);
        final FileInfo info = new FileInfo(snapshotFile.toPath(), md5);
        storage.updateLatestSnapshot(new SingleFileSnapshotInfo(info, last));

        return last.getIndex();
    }

    public long loadSnapshot(SingleFileSnapshotInfo snapshot) throws IOException {
        if (snapshot == null) {
            LOG.warn("The snapshot info is null.");
            return RaftLog.INVALID_LOG_INDEX;
        }
        final File snapshotFile = snapshot.getFile().getPath().toFile();
        if (!snapshotFile.exists()) {
            LOG.warn("The snapshot file {} does not exist for snapshot {}", snapshotFile, snapshot);
            return RaftLog.INVALID_LOG_INDEX;
        }

        // verify md5
        final MD5Hash md5 = snapshot.getFile().getFileDigest();
        if (md5 != null) {
            MD5FileUtil.verifySavedMD5(snapshotFile, md5);
        }

        final TermIndex last = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(snapshotFile);
        try(AutoCloseableLock writeLock = writeLock()){
            setLastAppliedTermIndex(last);
        }
        return last.getIndex();
    }

    @Override
    public StateMachineStorage getStateMachineStorage() {
        return storage;
    }

    @Override
    public CompletableFuture<Message> query(Message request) {
        final String q = request.toString();
        final String result;

        try(AutoCloseableLock writeLock = writeLock()) {
            result = SQLStorage.Execute(q);
        }

        System.out.println(result);

        return CompletableFuture.completedFuture(Message.valueOf(result));
    }

    @Override
    public void close() {
        SQLStorage.close();
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        final LogEntryProto entry = trx.getLogEntry();
        ByteString originData = entry.getStateMachineLogEntry().getLogData();
        String content = originData.toStringUtf8();

        final SQLMessage q = new SQLMessage(content);
        System.out.println("Apply Transaction:" + q.toString());

        String result = new String();

        System.out.println("Log Index:" + entry.getIndex());
        System.out.println("LastAppliedTerm and Index:" + getLastAppliedTermIndex().getTerm() + " " + getLastAppliedTermIndex().getIndex());

        try(AutoCloseableLock writeLock = writeLock()) {

            try {
                // 读TermIndex
                Properties props = new Properties();
                FileReader fis = new FileReader("TermIndex.properties");
                props.load(fis);
                String persist_term = props.getProperty("term");
                String persist_index = props.getProperty("index");
                fis.close();

                if(TermIndex.valueOf(entry.getTerm(),entry.getIndex()).compareTo(
                        TermIndex.valueOf(Long.parseLong(persist_term),Long.parseLong(persist_index)))
                        >0){
                    result = SQLStorage.Execute(q.toString());
                    // 写TermIndex
                    Properties props2 = new Properties();

                    props2.setProperty("term", String.valueOf(entry.getTerm()));
                    props2.setProperty("index", String.valueOf(entry.getIndex()));

                    props2.store(new FileWriter("TermIndex.properties"),null);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
        }

        System.out.println(result);

        final CompletableFuture<Message> f = CompletableFuture.completedFuture(Message.valueOf(result));


        final RaftPeerRole role = trx.getServerRole();
        if (role == RaftPeerRole.LEADER) {
            LOG.info(q.toString());
        } else {
            LOG.debug(q.toString());
        }

        return f;
    }

    public void notifyLeaderChanged(RaftGroupMemberId groupMemberId, RaftPeerId newLeaderId){

        Properties props = new Properties();
        try {
            // 从文件中读取配置信息
            FileInputStream fis = new FileInputStream("config.properties");
            props.load(fis);
            fis.close();

            // 获取属性值
            String raftGroupId = props.getProperty("raftGroupId");
            String peers = props.getProperty("peers");

            // 输出属性值
            System.out.println("Report：raftGroupId: " + raftGroupId);
            System.out.println("peers: " + peers);

            service.ReportRegion(raftGroupId,peers);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}