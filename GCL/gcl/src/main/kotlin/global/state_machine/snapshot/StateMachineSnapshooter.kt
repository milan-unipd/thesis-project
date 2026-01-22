package global.state_machine.snapshot

import org.apache.ratis.server.protocol.TermIndex
import org.apache.ratis.statemachine.SnapshotInfo
import java.io.File

abstract class StateMachineSnapshooter {

    internal var snapshotDir: File? = null
    internal var lastCommitedTerm: Long = 0
    internal var lastCommitedIndex: Long = -1

    protected abstract fun takeSnapshot(snapshotData: SnapshotData): Long
    abstract fun getLatestSnapshot(): SnapshotInfo?
    abstract fun loadFromSnapshot(): Pair<TermIndex, SnapshotData>?
    abstract fun writeExternalSnapshotToFile(externSnapshot: SnapshotData?): Result<Unit?>
}