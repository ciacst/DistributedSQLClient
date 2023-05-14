package org.example;

import com.codahale.metrics.Timer;

import java.io.IOException;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.SnapshotRetentionPolicy;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.Preconditions;

public class BaseStateMachine implements StateMachine, StateMachine.DataApi, StateMachine.EventApi, StateMachine.LeaderEventApi, StateMachine.FollowerEventApi {
    private final CompletableFuture<RaftServer> server = new CompletableFuture();
    private volatile RaftGroupId groupId;
    private final LifeCycle lifeCycle = new LifeCycle(JavaUtils.getClassSimpleName(this.getClass()));
    private final AtomicReference<TermIndex> lastAppliedTermIndex = new AtomicReference();
    private final SortedMap<Long, CompletableFuture<Void>> transactionFutures = new TreeMap();

    public BaseStateMachine() {
        this.setLastAppliedTermIndex(TermIndex.valueOf(0L, -1L));
    }

    public RaftPeerId getId() {
        return this.server.isDone() ? ((RaftServer)this.server.join()).getId() : null;
    }

    public LifeCycle getLifeCycle() {
        return this.lifeCycle;
    }

    public CompletableFuture<RaftServer> getServer() {
        return this.server;
    }

    public RaftGroupId getGroupId() {
        return this.groupId;
    }

    public LifeCycle.State getLifeCycleState() {
        return this.lifeCycle.getCurrentState();
    }

    public void initialize(RaftServer raftServer, RaftGroupId raftGroupId, RaftStorage storage) throws IOException {
        this.groupId = raftGroupId;
        this.server.complete(raftServer);
        this.lifeCycle.setName("" + this);
    }

    public SnapshotInfo getLatestSnapshot() {
        return this.getStateMachineStorage().getLatestSnapshot();
    }

    public void pause() {
    }

    public void reinitialize() throws IOException {
    }

    public TransactionContext applyTransactionSerial(TransactionContext trx) throws InvalidProtocolBufferException {
        return trx;
    }

    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        RaftProtos.LogEntryProto entry = (RaftProtos.LogEntryProto) Objects.requireNonNull(trx.getLogEntry());
        this.updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
        return CompletableFuture.completedFuture(Message.valueOf(trx.getLogEntry().getStateMachineLogEntry().getLogData()));
    }

    public TermIndex getLastAppliedTermIndex() {
        return (TermIndex)this.lastAppliedTermIndex.get();
    }

    protected void setLastAppliedTermIndex(TermIndex newTI) {
        this.lastAppliedTermIndex.set(newTI);
    }

    public void notifyTermIndexUpdated(long term, long index) {
        this.updateLastAppliedTermIndex(term, index);
    }

    protected boolean updateLastAppliedTermIndex(long term, long index) {
        return this.updateLastAppliedTermIndex(TermIndex.valueOf(term, index));
    }

    protected boolean updateLastAppliedTermIndex(TermIndex newTI) {
        Objects.requireNonNull(newTI, "newTI == null");
        TermIndex oldTI = (TermIndex)this.lastAppliedTermIndex.getAndSet(newTI);
        if (!newTI.equals(oldTI)) {
            LOG.trace("{}: update lastAppliedTermIndex from {} to {}", new Object[]{this.getId(), oldTI, newTI});
            if (oldTI != null) {
                Preconditions.assertTrue(newTI.compareTo(oldTI) >= 0, () -> {
                    return this.getId() + ": Failed updateLastAppliedTermIndex: newTI = " + newTI + " < oldTI = " + oldTI;
                });
            }

            return true;
        } else {
            synchronized(this.transactionFutures) {
                long i;
                while(!this.transactionFutures.isEmpty() && (i = (Long)this.transactionFutures.firstKey()) <= newTI.getIndex()) {
                    ((CompletableFuture)this.transactionFutures.remove(i)).complete((Object)null);
                }

                return false;
            }
        }
    }

    public long takeSnapshot() throws IOException {
        return -1L;
    }

    public StateMachineStorage getStateMachineStorage() {
        return new StateMachineStorage() {
            public void init(RaftStorage raftStorage) throws IOException {
            }

            public SnapshotInfo getLatestSnapshot() {
                return null;
            }

            public void format() throws IOException {
            }

            public void cleanupOldSnapshots(SnapshotRetentionPolicy snapshotRetentionPolicy) {
            }
        };
    }

    public CompletableFuture<Message> queryStale(Message request, long minIndex) {
        if (this.getLastAppliedTermIndex().getIndex() < minIndex) {
            synchronized(this.transactionFutures) {
                if (this.getLastAppliedTermIndex().getIndex() < minIndex) {
                    return ((CompletableFuture)this.transactionFutures.computeIfAbsent(minIndex, (key) -> {
                        return new CompletableFuture();
                    })).thenCompose((v) -> {
                        return this.query(request);
                    });
                }
            }
        }

        return this.query(request);
    }

    public CompletableFuture<Message> query(Message request) {
        return CompletableFuture.completedFuture((Message)null);
    }

    public TransactionContext startTransaction(RaftClientRequest request) throws IOException {
        return TransactionContext.newBuilder().setStateMachine(this).setClientRequest(request).build();
    }

    public TransactionContext cancelTransaction(TransactionContext trx) throws IOException {
        return trx;
    }

    public TransactionContext preAppendTransaction(TransactionContext trx) throws IOException {
        return trx;
    }

    public void close() throws IOException {
    }

    public String toString() {
        return JavaUtils.getClassSimpleName(this.getClass()) + ":" + (!this.server.isDone() ? "uninitialized" : this.getId() + ":" + this.groupId);
    }

    protected CompletableFuture<Message> recordTime(Timer timer, Task task) {
        Timer.Context timerContext = timer.time();

        CompletableFuture var4;
        try {
            var4 = task.run();
        } finally {
            timerContext.stop();
        }

        return var4;
    }

    protected interface Task {
        CompletableFuture<Message> run();
    }
}

