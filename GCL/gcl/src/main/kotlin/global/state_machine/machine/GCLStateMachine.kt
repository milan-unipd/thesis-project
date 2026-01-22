package global.state_machine.machine

import gcl.MasterClusterInfo
import global.state_machine.LockEntry
import global.state_machine.commands.*
import global.state_machine.snapshot.GCLSnapshooter
import global.state_machine.snapshot.SnapshotData
import kotlinx.serialization.json.Json
import org.apache.ratis.protocol.Message
import org.apache.ratis.protocol.RaftGroupId
import org.apache.ratis.server.RaftServer
import org.apache.ratis.server.storage.RaftStorage
import org.apache.ratis.statemachine.SnapshotInfo
import org.apache.ratis.statemachine.TransactionContext
import org.apache.ratis.statemachine.impl.BaseStateMachine
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

class GCLStateMachine(
    private val externSnapshot: SnapshotData?,
    private val responder: StateMachineEventResponder,
) : BaseStateMachine() {

    private val masterClusterInfo: AtomicReference<MasterClusterInfo?> = AtomicReference()
    internal val locks = ConcurrentHashMap<String, LockEntry>()
    internal var logicalTime: AtomicLong = AtomicLong(0)
    internal var isBootstrapped = false
        private set
    private val norr = AtomicInteger(0)


    private val snapshooter = GCLSnapshooter()


    override fun initialize(raftServer: RaftServer?, raftGroupId: RaftGroupId?, storage: RaftStorage?) {
        super.initialize(raftServer, raftGroupId, storage)
        snapshooter.snapshotDir = storage?.storageDir?.stateMachineDir
        if (externSnapshot != null) {
            snapshooter.writeExternalSnapshotToFile(externSnapshot)

        }
        loadStateFromSnapshot()
    }

    override fun reinitialize() {
        loadStateFromSnapshot()
    }

    private fun loadStateFromSnapshot() {
        val (termIndex, snapshot) = snapshooter.loadFromSnapshot() ?: return
        updateLastAppliedTermIndex(termIndex)
        masterClusterInfo.set(snapshot.masterClusterInfo)
        locks.putAll(snapshot.locks)
        logicalTime.set(snapshot.logicalTime)
        isBootstrapped = snapshot.isBootstrapped
    }

    override fun notifyLeaderReady() {
        responder.onLeader()
        super.notifyLeaderReady()

    }

    override fun notifyNotLeader(pendingEntries: Collection<TransactionContext?>?) {
        responder.onNotLeader()
        super.notifyNotLeader(pendingEntries)
    }


    override fun applyTransaction(trx: TransactionContext?): CompletableFuture<Message> {
        if (trx == null)
            return CompletableFuture.completedFuture(null)


        val cmdBytes = trx.logEntry.stateMachineLogEntry.logData.toByteArray()

        try {
            val result = when (val cmd = CommandCodec.deserialize(cmdBytes)) {

                is ClusterBootstrap -> {
                    if (isBootstrapped) return CompletableFuture.completedFuture(Message.valueOf("ALREADY_BOOTSTRAPPED"))
                    isBootstrapped = true
                    logicalTime.set(cmd.initialLogicalTime)
                    locks["leader-lock"] = LockEntry(
                        cmd.initialLeaderLockOwner,
                        cmd.initialLogicalTime + 60
                    )
                    masterClusterInfo.set(cmd.masterInfo)

                    Message.valueOf("BOOTSTRAPPED")
                }

                else -> {
                    if (!isBootstrapped)
                        return CompletableFuture.completedFuture(Message.valueOf("NOT BOOTSTRAPPED YET"))
                    when (cmd) {
                        is LockCommand -> lockLogic(cmd)
                        is MasterClusterInfoCommand -> masterInfoLogic(cmd)
                        is NoRRCommand -> norrLogic(cmd)
                    }
                }
            }

            snapshooter.apply {
                lastCommitedTerm = trx.logEntry.term
                lastCommitedIndex = trx.logEntry.index
            }
            return CompletableFuture.completedFuture(result)
        } catch (e: Exception) {
            return CompletableFuture.failedFuture(e)
        }
    }

    private fun lockLogic(cmd: LockCommand): Message {
        val value = when (cmd) {
            is LockCommand.Acquire -> acquireLock(cmd)
            is LockCommand.Release -> releaseLock(cmd)
            is LockCommand.Tick -> {
                val time = logicalTime.incrementAndGet()
                expireLocks()
                "TICK:$time"
            }
        }
        return Message.valueOf(value)
    }

    private fun masterInfoLogic(cmd: MasterClusterInfoCommand): Message {
        val value = when (cmd) {
            is MasterClusterInfoCommand.UpdateMasterClusterInfo -> {
                masterClusterInfo.set(cmd.masterClusterInfo)
                "UPDATED"
            }

            is MasterClusterInfoCommand.GetMasterClusterInfo -> Json.encodeToString(masterClusterInfo.get())
        }
        return Message.valueOf(value)
    }

    private fun norrLogic(cmd: NoRRCommand): Message {
        val value = when (cmd) {
            is NoRRCommand.GetNoRR -> norr.get().toString()
            is NoRRCommand.SetNoRR -> {
                if (norr.compareAndSet(cmd.old, cmd.new))
                    "OK"
                else
                    "FAIL"
            }
        }
        return Message.valueOf(value)
    }

    override fun query(request: Message): CompletableFuture<Message> {
        super.query(request)
        val query = CommandCodec.deserialize(request.content.toByteArray())
        val result = when (query) {
            is MasterClusterInfoCommand.GetMasterClusterInfo -> masterClusterInfo.get()?.let { Json.encodeToString(it) }
                ?: return CompletableFuture.failedFuture(RuntimeException("gcl.MasterClusterInfo does not exist."))

            is NoRRCommand.GetNoRR -> norr.get().toString()

            is ClusterBootstrap,
            is LockCommand.Acquire,
            is LockCommand.Release,
            is LockCommand.Tick,
            is MasterClusterInfoCommand.UpdateMasterClusterInfo,
            is NoRRCommand.SetNoRR -> return CompletableFuture.failedFuture(Exception("CANT QUERY THIS TYPE OF COMMAND"))


        }
        return CompletableFuture.completedFuture(Message.valueOf(result))
    }

    override fun getLatestSnapshot(): SnapshotInfo? = snapshooter.getLatestSnapshot()

    override fun takeSnapshot(): Long = snapshooter.takeSnapshot(
        SnapshotData(
            snapshooter.lastCommitedTerm,
            snapshooter.lastCommitedIndex,
            masterClusterInfo.get(),
            locks.toMap(),
            logicalTime.get(),
            isBootstrapped,
            norr.get()
        )
    )

    fun getLastSavedSnapshotData() = snapshooter.getLastSavedSnapshotData()
}