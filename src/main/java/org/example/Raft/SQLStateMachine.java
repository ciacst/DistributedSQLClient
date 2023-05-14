package org.example.Raft;

import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

        final long index = entry.getIndex();
        final String result;

        System.out.println("Log Index:" + index);

        try(AutoCloseableLock writeLock = writeLock()) {
            result = SQLStorage.Execute(q.toString());
            updateLastAppliedTermIndex(entry.getTerm(), index);
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
}