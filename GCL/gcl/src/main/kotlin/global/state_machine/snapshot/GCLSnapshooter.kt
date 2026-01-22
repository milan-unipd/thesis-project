package global.state_machine.snapshot

import org.apache.ratis.io.MD5Hash
import org.apache.ratis.server.protocol.TermIndex
import org.apache.ratis.server.raftlog.RaftLog
import org.apache.ratis.server.storage.FileInfo
import org.apache.ratis.statemachine.SnapshotInfo
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo
import java.io.File
import java.util.concurrent.atomic.AtomicReference

class GCLSnapshooter : StateMachineSnapshooter() {

    private val lastSavedSnapshotData = AtomicReference<SnapshotData?>(null)


    public override fun takeSnapshot(snapshotData: SnapshotData): Long {
        if (snapshotDir == null) return RaftLog.INVALID_LOG_INDEX

        val term = lastCommitedTerm
        val index = lastCommitedIndex

        val snapshotFile = File(snapshotDir, "snapshot_${term}_$index.dat")
        snapshotDir!!.mkdirs()

        snapshotFile.writeBytes(snapshotData.serialize())
        lastSavedSnapshotData.set(snapshotData)

        snapshotDir!!.listFiles()?.forEach { file -> if (file != snapshotFile) file.delete() }
        return index

    }

    override fun getLatestSnapshot(): SnapshotInfo? {
        if (snapshotDir == null || !snapshotDir!!.exists()) {
            return null
        }

        val (term, index, snapshotFile) = File(snapshotDir!!.path)
            .listFiles()
            .filter { it.name.endsWith(".dat") }
            .map {
                val term = it.name.split("_")[1].toLong()
                val index = it.name.split("_")[2].split(".")[0].toLong()

                Triple(term, index, it)
            }.maxWithOrNull(compareBy({ it.first }, { it.second })) ?: return null

        if (snapshotFile == null || !snapshotFile.exists())
            return null

        val termIndex = TermIndex.valueOf(term, index)

        val fileInfo = FileInfo(snapshotFile.toPath(), MD5Hash.digest(snapshotFile.inputStream()))
        return SingleFileSnapshotInfo(fileInfo, termIndex)
    }

    override fun loadFromSnapshot(): Pair<TermIndex, SnapshotData>? {
        val latestSnapshot = getLatestSnapshot()
        if (latestSnapshot != null && latestSnapshot is SingleFileSnapshotInfo) {
            val snapshotFile = latestSnapshot.file.path.toFile()
            if (snapshotFile.exists()) {
                val snapshot = SnapshotData.deserialize(snapshotFile.readBytes())
                return Pair(latestSnapshot.termIndex, snapshot)
            }
        }
        return null
    }

    fun getLastSavedSnapshotData() = lastSavedSnapshotData.get()

    override fun writeExternalSnapshotToFile(externSnapshot: SnapshotData?) = runCatching {
        if (snapshotDir == null || externSnapshot == null)
            return@runCatching null
        File(snapshotDir, "snapshot_${externSnapshot.lastCommitedTerm}_${externSnapshot.lastCommitedIndex}.dat").writeBytes(
            externSnapshot.serialize()
        )
    }


}